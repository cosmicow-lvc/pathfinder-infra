# Contrato: formato de error común y correlationId

Aplica a todos los servicios: **bff-gateway**, **ms-auth**, **ms-rutas**,
**ms-buses** y **mcp-optimizer**. Complementa a `schema.graphql`, `auth.proto`,
`kafka-topics.md` y `mcp-optimize.yaml`.

---

## 1. Formato de error común

Un error siempre tiene un `code` (lo lee el código) y un `message` (lo lee una persona).
Cambia solo el sobre, según el protocolo:

| Protocolo | Cómo viaja el error |
|---|---|
| Kafka | `{ "ok": false, "error": { "code": "NOT_FOUND", "message": "..." } }` |
| REST (MCP) | `{ "error": { "code", "message" }, "correlationId": "..." }` + código HTTP |
| gRPC (Auth) | Código de estado gRPC + mensaje (`RpcException` en C#) |
| GraphQL (al frontend) | `errors[0].message` y `errors[0].extensions.{ code, correlationId }` |

### Tabla de equivalencias

| `code` común | gRPC | HTTP | GraphQL `extensions.code` |
|---|---|---|---|
| `BAD_REQUEST` | `INVALID_ARGUMENT` | 400 | `BAD_REQUEST` |
| `UNAUTHENTICATED` | `UNAUTHENTICATED` | 401 | `UNAUTHENTICATED` |
| `FORBIDDEN` | `PERMISSION_DENIED` | 403 | `FORBIDDEN` |
| `NOT_FOUND` | `NOT_FOUND` | 404 | `NOT_FOUND` |
| `CONFLICT` (duplicados) | `ALREADY_EXISTS` | 409 | `BAD_REQUEST` |
| `BUS_NOT_FOUND` | solo Kafka | solo Kafka | `BAD_REQUEST` |
| `BUS_NOT_ACTIVE` | solo Kafka | solo Kafka | `BAD_REQUEST` |
| `UPSTREAM_ERROR` (otro servicio o Claude falló) | `UNAVAILABLE` | 502 | `SERVICE_UNAVAILABLE` |
| `TIMEOUT` | `DEADLINE_EXCEEDED` | 504 | `TIMEOUT` |
| `INTERNAL` | `INTERNAL` | 500 | `INTERNAL_SERVER_ERROR` |

### Lo que ve el frontend

```json
{
  "errors": [{
    "message": "El bus no está activo",
    "extensions": { "code": "BAD_REQUEST", "correlationId": "e3a1c9d0-5b7e-4c1a-9d2f-0a6b8c4e1f33" }
  }]
}
```

### Reglas de contenido

- Mensajes en español, comprensibles sin conocer el sistema.
- Nunca se envía al cliente un stack trace, una consulta SQL ni un nombre interno. Eso va solo a los logs.
- Los errores inesperados (`INTERNAL`) llegan al cliente con un mensaje genérico ("Error interno del servidor") y el `correlationId`.
- En el login, el error no revela si falló el email o la contraseña (HU-02).
- Cada servicio traduce **sus** errores a esta tabla. El BFF los traduce a GraphQL en un solo filtro global; los resolvers no manejan errores a mano.

---

## 2. correlationId

Un identificador único por petición que viaja por todos los servicios por los que pasa.
Si alguien reporta un error, se busca ese id en los logs de cada servicio y se reconstruye el recorrido.

| Salto | Dónde va |
|---|---|
| Frontend → BFF | Header HTTP `x-correlation-id` (opcional; si no viene, el BFF lo crea) |
| BFF → Auth | Metadata gRPC `x-correlation-id` |
| BFF → Rutas / Buses | Header Kafka `x-correlation-id` |
| Rutas → Buses (`bus.validate`) | El mismo header, copiado de la petición original |
| Eventos (`bus.status.changed`, `travel.created`) | Header Kafka y campo `correlationId` del mensaje |
| BFF → MCP | Header HTTP `x-correlation-id` |

> **Ojo con Kafka:** hay dos ids distintos.
> `kafka_correlationId` solo une una respuesta con su petición (lo manejan NestJS y Spring Kafka).
> `x-correlation-id` es el de trazabilidad. Son valores distintos y ambos deben ir en cada mensaje.

### Reglas

1. El BFF lo genera (UUID v4) al recibir la petición, salvo que el cliente ya envíe uno.
2. Cada servicio lo lee al entrar, lo incluye en **todas** sus líneas de log y lo reenvía en cada llamada saliente.
3. Si un servicio recibe una petición sin id, genera uno propio y lo registra.
4. Se devuelve siempre al cliente: header de respuesta `x-correlation-id` y `extensions.correlationId` en los errores GraphQL.

### Formato de logs (JSON, una línea por evento)

```json
{"ts":"2026-11-19T14:30:00Z","level":"error","service":"ms-rutas","correlationId":"e3a1c9d0-...","msg":"bus.validate sin respuesta"}
```

Los campos mínimos son `ts`, `level`, `service`, `correlationId` y `msg`.

---

## 3. Implementación en NestJS (bff-gateway)

### 3.1 `common/errors/app-error.ts`

```ts
export type ErrorCode =
  | 'BAD_REQUEST' | 'UNAUTHENTICATED' | 'FORBIDDEN' | 'NOT_FOUND' | 'CONFLICT'
  | 'BUS_NOT_FOUND' | 'BUS_NOT_ACTIVE' | 'UPSTREAM_ERROR' | 'TIMEOUT' | 'INTERNAL';

export class AppError extends Error {
  constructor(public readonly code: ErrorCode, message: string) {
    super(message);
    this.name = 'AppError';
  }
}

// code común -> extensions.code de GraphQL
export const GRAPHQL_CODE: Record<ErrorCode, string> = {
  BAD_REQUEST: 'BAD_REQUEST',
  UNAUTHENTICATED: 'UNAUTHENTICATED',
  FORBIDDEN: 'FORBIDDEN',
  NOT_FOUND: 'NOT_FOUND',
  CONFLICT: 'BAD_REQUEST',
  BUS_NOT_FOUND: 'BAD_REQUEST',
  BUS_NOT_ACTIVE: 'BAD_REQUEST',
  UPSTREAM_ERROR: 'SERVICE_UNAVAILABLE',
  TIMEOUT: 'TIMEOUT',
  INTERNAL: 'INTERNAL_SERVER_ERROR',
};
```

### 3.2 `common/correlation/correlation.ts` (middleware + almacenamiento)

Se usa `AsyncLocalStorage` para leer el id desde cualquier servicio sin pasarlo por parámetro.
El middleware corre antes que los guards, así que el guard de autenticación también lo tiene disponible.

```ts
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { Injectable, NestMiddleware } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';

const store = new AsyncLocalStorage<{ correlationId: string }>();

export const getCorrelationId = (): string | undefined => store.getStore()?.correlationId;

@Injectable()
export class CorrelationMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction) {
    const header = req.headers['x-correlation-id'];
    const id = (Array.isArray(header) ? header[0] : header) || randomUUID();
    (req as any).correlationId = id;          // respaldo: el filtro también puede leerlo de req
    res.setHeader('x-correlation-id', id);
    store.run({ correlationId: id }, () => next());
  }
}
```

### 3.3 `common/errors/mappers.ts` (cada protocolo -> `AppError`)

```ts
import { status } from '@grpc/grpc-js';
import { AppError, ErrorCode } from './app-error';

// gRPC (Auth)
const GRPC_TO_CODE: Record<number, ErrorCode> = {
  [status.INVALID_ARGUMENT]: 'BAD_REQUEST',
  [status.UNAUTHENTICATED]: 'UNAUTHENTICATED',
  [status.PERMISSION_DENIED]: 'FORBIDDEN',
  [status.NOT_FOUND]: 'NOT_FOUND',
  [status.ALREADY_EXISTS]: 'CONFLICT',
  [status.UNAVAILABLE]: 'UPSTREAM_ERROR',
  [status.DEADLINE_EXCEEDED]: 'TIMEOUT',
};

export function fromGrpcError(e: any): AppError {
  const code = GRPC_TO_CODE[e?.code] ?? 'INTERNAL';
  return new AppError(code, code === 'INTERNAL' ? 'Error interno del servidor' : (e.details ?? e.message));
}

// REST (MCP) con @nestjs/axios
export function fromHttpError(e: any): AppError {
  if (['ECONNABORTED', 'ETIMEDOUT'].includes(e?.code)) {
    return new AppError('TIMEOUT', 'El optimizador tardó demasiado en responder');
  }
  const body = e?.response?.data?.error;
  if (body?.code) return new AppError(body.code, body.message);
  return new AppError('UPSTREAM_ERROR', 'El optimizador no está disponible');
}
```

### 3.4 `common/kafka/kafka-requester.ts` (request-reply con envelope, timeout y headers)

Usa `firstValueFrom` con `timeout`. Con `lastValueFrom` la llamada queda colgada cuando
responde un servicio que no es Nest (Java o Go).

```ts
import { Inject, Injectable } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { randomUUID } from 'node:crypto';
import { firstValueFrom, timeout, TimeoutError } from 'rxjs';
import { AppError, ErrorCode } from '../errors/app-error';
import { getCorrelationId } from '../correlation/correlation';

type Envelope<T> =
  | { ok: true; data: T }
  | { ok: false; error: { code: ErrorCode; message: string } };

@Injectable()
export class KafkaRequester {
  constructor(@Inject('KAFKA_CLIENT') private readonly client: ClientKafka) {}
  // Recordar: en onModuleInit llamar a client.subscribeToResponseOf(topic) por cada topic
  // y luego await client.connect().

  async request<T>(topic: string, data: unknown, timeoutMs = 5000): Promise<T> {
    let reply: Envelope<T>;
    try {
      reply = await firstValueFrom(
        this.client
          .send<Envelope<T>>(topic, {
            value: data,
            headers: { 'x-correlation-id': getCorrelationId() ?? randomUUID() },
          })
          .pipe(timeout(timeoutMs)),
      );
    } catch (e) {
      if (e instanceof TimeoutError) {
        throw new AppError('TIMEOUT', 'El servicio tardó demasiado en responder');
      }
      throw new AppError('UPSTREAM_ERROR', 'El servicio no está disponible');
    }
    if (!reply.ok) throw new AppError(reply.error.code, reply.error.message);
    return reply.data;
  }
}
```

### 3.5 Llamada gRPC con metadata (ejemplo en el cliente de Auth)

```ts
import { Metadata } from '@grpc/grpc-js';
import { firstValueFrom, timeout } from 'rxjs';

const md = new Metadata();
md.set('x-correlation-id', getCorrelationId() ?? '');

try {
  return await firstValueFrom(this.authService.login(input, md).pipe(timeout(5000)));
} catch (e) {
  throw fromGrpcError(e);
}
```

### 3.6 `common/errors/all-exceptions.filter.ts` (filtro global)

```ts
import { ArgumentsHost, Catch, HttpException, Logger } from '@nestjs/common';
import { GqlArgumentsHost, GqlExceptionFilter } from '@nestjs/graphql';
import { GraphQLError } from 'graphql';
import { AppError, ErrorCode, GRAPHQL_CODE } from './app-error';
import { getCorrelationId } from '../correlation/correlation';

const HTTP_TO_CODE: Record<number, ErrorCode> = {
  400: 'BAD_REQUEST', 401: 'UNAUTHENTICATED', 403: 'FORBIDDEN', 404: 'NOT_FOUND', 409: 'CONFLICT',
};

@Catch()
export class AllExceptionsFilter implements GqlExceptionFilter {
  private readonly logger = new Logger(AllExceptionsFilter.name);

  catch(exception: unknown, host: ArgumentsHost) {
    const ctx = GqlArgumentsHost.create(host).getContext();
    const correlationId = getCorrelationId() ?? ctx?.req?.correlationId;
    const { code, message } = this.normalize(exception);

    if (code === 'INTERNAL' || code === 'UPSTREAM_ERROR') {
      this.logger.error(
        JSON.stringify({ correlationId, code, message: (exception as Error)?.message }),
        (exception as Error)?.stack,
      );
    }

    // Devolver el error (no lanzarlo) es el patrón de los filtros de GraphQL en Nest
    return new GraphQLError(message, {
      extensions: { code: GRAPHQL_CODE[code], correlationId },
    });
  }

  private normalize(e: unknown): { code: ErrorCode; message: string } {
    if (e instanceof AppError) return { code: e.code, message: e.message };

    if (e instanceof HttpException) {
      const code = HTTP_TO_CODE[e.getStatus()] ?? 'INTERNAL';
      if (code === 'INTERNAL') return { code, message: 'Error interno del servidor' };
      const res = e.getResponse() as any;
      const msg = typeof res === 'string' ? res : Array.isArray(res?.message) ? res.message.join('; ') : (res?.message ?? e.message);
      return { code, message: msg };
    }

    return { code: 'INTERNAL', message: 'Error interno del servidor' };
  }
}
```

### 3.7 Registro en `app.module.ts`

```ts
@Module({
  imports: [
    GraphQLModule.forRoot<ApolloDriverConfig>({
      driver: ApolloDriver,
      typePaths: ['./contracts/schema.graphql'],
      context: ({ req, res }) => ({ req, res }),
    }),
    // ...módulos de Kafka, gRPC, HTTP
  ],
  providers: [{ provide: APP_FILTER, useClass: AllExceptionsFilter }],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer) {
    consumer
      .apply(CorrelationMiddleware)
      .forRoutes({ path: 'graphql', method: RequestMethod.ALL });
  }
}
```

Uso en un servicio: lanzar `throw new AppError('NOT_FOUND', 'La ruta no existe')`, o dejar que
el requester lance los errores de los microservicios. El filtro hace el resto.

---

## 4. Implementación en los demás servicios

### ms-rutas

Leer el header del mensaje, ponerlo en el MDC de SLF4J (para que salga en cada log) y reenviarlo
en las llamadas salientes (por ejemplo `bus.validate`).

```java
@KafkaListener(topics = "rutas.city.list", groupId = "ms-rutas")
@SendTo
public Envelope<List<CityDto>> listCities(
        @Payload CityFilter filter,
        @Header(name = "x-correlation-id", required = false) String correlationId) {
    MDC.put("correlationId", correlationId != null ? correlationId : UUID.randomUUID().toString());
    try {
        return Envelope.ok(cityService.list(filter));
    } catch (NotFoundException e) {
        return Envelope.error("NOT_FOUND", e.getMessage());
    } finally {
        MDC.clear();
    }
}
```

`Envelope` es una clase propia con `ok`, `data` y `error`. Con `ReplyingKafkaTemplate` y `@SendTo`,
Spring Kafka copia `kafka_correlationId` a la respuesta por ti.

### ms-buses

El id viaja dentro del `context.Context` y se lee de los headers del mensaje Kafka.

```go
type ctxKey struct{}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

func CorrelationID(ctx context.Context) string {
	v, _ := ctx.Value(ctxKey{}).(string)
	return v
}

// Al recibir un mensaje (segmentio/kafka-go):
func correlationFromHeaders(headers []kafka.Header) string {
	for _, h := range headers {
		if h.Key == "x-correlation-id" {
			return string(h.Value)
		}
	}
	return uuid.NewString()
}
```

Al responder, además de `x-correlation-id`, hay que devolver el header `kafka_correlationId`
**con el mismo valor que llegó** y publicar en el topic indicado por `kafka_replyTopic`.

### ms-auth

```csharp
public class CorrelationInterceptor : Interceptor
{
    private readonly ILogger<CorrelationInterceptor> _logger;
    public CorrelationInterceptor(ILogger<CorrelationInterceptor> logger) => _logger = logger;

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var id = context.RequestHeaders.GetValue("x-correlation-id") ?? Guid.NewGuid().ToString();
        using (_logger.BeginScope(new Dictionary<string, object> { ["correlationId"] = id }))
        {
            return await continuation(request, context);
        }
    }
}

// Program.cs
builder.Services.AddGrpc(o => o.Interceptors.Add<CorrelationInterceptor>());

// Errores: lanzar RpcException con el código de la tabla
throw new RpcException(new Status(StatusCode.AlreadyExists, "El email ya está registrado"));
```

### mcp-optimizer

```ts
app.use((req, res, next) => {
  const id = req.header('x-correlation-id') ?? randomUUID();
  res.locals.correlationId = id;
  res.setHeader('x-correlation-id', id);
  next();
});

// Manejador de errores (al final de todas las rutas)
app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
  const code = err.code ?? 'INTERNAL';
  const http = { BAD_REQUEST: 400, UPSTREAM_ERROR: 502, TIMEOUT: 504 }[code as string] ?? 500;
  res.status(http).json({
    error: { code, message: http === 500 ? 'Error interno del servidor' : err.message },
    correlationId: res.locals.correlationId,
  });
});
```

---

## 5. Cómo comprobar que funciona

1. **Id de punta a punta:** enviar una mutation con el header `x-correlation-id: prueba-123` y comprobar que
   `prueba-123` aparece en los logs del BFF, de Rutas y de Buses (por ejemplo al crear un viaje).
2. **Id generado:** enviar la misma petición sin header y comprobar que el BFF lo crea y lo devuelve en el header de respuesta.
3. **Error de negocio:** crear un viaje con un bus en mantención y verificar `extensions.code = "BAD_REQUEST"` y el mensaje en español.
4. **Timeout:** detener ms-buses y repetir la llamada; el BFF debe responder un error `TIMEOUT` o `SERVICE_UNAVAILABLE` en unos 5 s, sin colgarse.
5. **Error interno:** forzar una excepción en un servicio y verificar que el cliente solo ve "Error interno del servidor" y el id, mientras que el stack trace queda en los logs.
6. **Tests del BFF:** un test unitario del filtro (un `AppError` por cada código de la tabla) y otro del `KafkaRequester` (respuesta `ok: false` y timeout), lo que además suma cobertura.