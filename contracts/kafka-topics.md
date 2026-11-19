# Contrato Kafka: topics y eventos

Participantes: **bff-gateway** (NestJS), **ms-rutas** (Java) y **ms-buses** (Go).
Auth (gRPC) y MCP (REST) no usan Kafka.

## 1. Convenciones

| Tema | Regla |
|---|---|
| Nombres BFF → servicio | `<servicio>.<recurso>.<accion>` (ej. `rutas.city.create`) |
| Nombres entre servicios | `<recurso>.<accion>` o evento en pasado (ej. `bus.validate`, `bus.status.changed`) |
| Request-reply | La respuesta va a `<topic>.reply` |
| Formato de mensajes | JSON, campos en camelCase, ids como string, fechas en ISO 8601 |
| Partición/replicación (local) | 1 partición y factor de replicación 1 en todos los topics |
| Consumer groups | `ms-rutas`, `ms-buses`, `bff-gateway-client` (respuestas) |
| Clave del mensaje | `busId` en eventos de buses (mantiene el orden por bus) |

Los nombres de los campos coinciden con `schema.graphql`.

## 2. Headers obligatorios

Los mismos nombres que usan NestJS y Spring Kafka, para que los tres lenguajes se entiendan:

| Header | Quién lo pone | Para qué |
|---|---|---|
| `kafka_correlationId` | Quien pide | Une la respuesta con su petición. El que responde debe devolverlo **idéntico** |
| `kafka_replyTopic` | Quien pide | Topic donde se espera la respuesta (`<topic>.reply`) |
| `x-correlation-id` | BFF, y se propaga | Id de trazabilidad de punta a punta (el mismo que en gRPC y REST) |

En Go hay que leerlos y escribirlos a mano. En Spring Kafka, `ReplyingKafkaTemplate` y `@SendTo` los manejan solos.

## 3. Formato de respuesta (envelope)

Todas las respuestas a `.reply` usan el mismo formato, para que el BFF no dependa del lenguaje del servicio:

```json
{ "ok": true, "data": { } }
```
```json
{ "ok": false, "error": { "code": "NOT_FOUND", "message": "Ciudad 5 no existe" } }
```

| `error.code` | Cuándo | Código GraphQL en el BFF |
|---|---|---|
| `BAD_REQUEST` | Datos inválidos (origen = destino, campo vacío) | `BAD_REQUEST` |
| `NOT_FOUND` | El recurso no existe | `NOT_FOUND` |
| `CONFLICT` | Duplicado (patente, RUT, ciudad, ruta) | `BAD_REQUEST` |
| `BUS_NOT_FOUND` | El bus del viaje no existe | `BAD_REQUEST` |
| `BUS_NOT_ACTIVE` | El bus existe pero no está activo | `BAD_REQUEST` |
| `INTERNAL` | Error inesperado | `INTERNAL_SERVER_ERROR` |

Timeout en el BFF: **5 segundos** por petición. Si no hay respuesta, devuelve un error controlado.

## 4. Topics BFF → ms-rutas (request-reply)

Todos tienen su `<topic>.reply`.

| Topic | Request (`data`) | Respuesta (`data`) |
|---|---|---|
| `rutas.city.list` | `{ pais? }` | `City[]` |
| `rutas.city.get` | `{ id }` | `City` |
| `rutas.city.create` | `{ nombre, pais, region }` | `City` |
| `rutas.city.update` | `{ id, nombre, pais, region }` | `City` |
| `rutas.city.delete` | `{ id }` | `{ deleted: true }` |
| `rutas.ruta.list` | `{ inicioId?, destinoId? }` | `Ruta[]` (con `inicio` y `destino` como `City`) |
| `rutas.ruta.get` | `{ id }` | `Ruta` |
| `rutas.ruta.create` | `{ inicioId, destinoId }` | `Ruta` |
| `rutas.ruta.delete` | `{ id }` | `{ deleted: true }` |
| `rutas.travel.list` | `{ inicioId?, destinoId?, diaOperacion?, soloActivos }` | `Travel[]` con `busId` (sin objeto `bus`) |
| `rutas.travel.get` | `{ id }` | `Travel` con `busId` |
| `rutas.travel.create` | `{ rutaId, busId, costo, divisa, duracionMinutos, horaSalida, diasOperacion }` | `Travel` con `busId` |
| `rutas.travel.update` | `{ id, costo?, divisa?, duracionMinutos?, horaSalida?, diasOperacion?, activo? }` | `Travel` con `busId` |

Rutas solo devuelve el `busId`. El BFF completa el objeto `bus` consultando a ms-buses (ver sección 5).

## 5. Topics BFF → ms-buses (request-reply)

| Topic | Request (`data`) | Respuesta (`data`) |
|---|---|---|
| `buses.driver.list` | `{}` | `Driver[]` |
| `buses.driver.create` | `{ nombre, telefono, rut }` | `Driver` |
| `buses.bus.list` | `{ estado?, ids? }` | `Bus[]` (con `driver`) |
| `buses.bus.get` | `{ id }` | `Bus` |
| `buses.bus.create` | `{ patente, empresa, driverId }` | `Bus` |
| `buses.bus.change-status` | `{ id, estado }` | `Bus` |

`ids` en `buses.bus.list` permite traer todos los buses de una lista de viajes en **una** llamada, en vez de una por viaje.

## 6. Comunicación entre microservicios (bidireccional)

### 6.1 Rutas → Buses: `bus.validate` (request-reply)

Se usa al crear un viaje (HU-16). Reply en `bus.validate.reply`.

Request:
```json
{ "busId": "7c9e6679-7425-40de-944b-e07fc1f90ae7" }
```
Respuesta:
```json
{ "ok": true, "data": { "busId": "7c9e6679-...", "exists": true, "estado": "ACTIVO", "available": true } }
```

`available` es `true` solo si `estado == ACTIVO`. Rutas decide: si `exists = false` responde `BUS_NOT_FOUND`; si `available = false`, `BUS_NOT_ACTIVE`. En ambos casos el viaje no se crea.

### 6.2 Buses → Rutas: `bus.status.changed` (evento)

Se publica cada vez que cambia el estado de un bus (HU-14). Sin respuesta.

```json
{
  "eventId": "b1f0c4e2-...",
  "eventType": "bus.status.changed",
  "occurredAt": "2026-11-19T14:30:00Z",
  "correlationId": "e3a1...",
  "data": {
    "busId": "7c9e6679-...",
    "estadoAnterior": "ACTIVO",
    "estadoNuevo": "EN_MANTENCION"
  }
}
```

Consumidor: **ms-rutas**. Si `estadoNuevo != ACTIVO`, pone `activo = false` en todos los viajes de ese bus (HU-19). No los reactiva solo cuando el bus vuelve a `ACTIVO`: lo hace el admin con `updateTravel`.

### 6.3 Rutas → Buses: `travel.created` (evento, opcional)

Se publica al crear un viaje. Sirve como segundo sentido de comunicación si quieres reforzar el requisito. Si vas justo de tiempo, puedes omitirlo.

```json
{
  "eventId": "...",
  "eventType": "travel.created",
  "occurredAt": "2026-11-19T14:35:00Z",
  "correlationId": "...",
  "data": { "travelId": "...", "rutaId": "...", "busId": "7c9e6679-..." }
}
```

Consumidor: **ms-buses**, que lo registra (por ejemplo, para impedir que se desactive un bus con viajes activos).

## 7. Reglas para los consumidores

- **Idempotencia:** Kafka puede entregar un mensaje más de una vez. Los consumidores de eventos deben guardar los `eventId` procesados, o hacer operaciones que den el mismo resultado si se repiten (desactivar un viaje ya inactivo no cambia nada).
- **Mensajes inválidos:** si el JSON no se puede leer, registrar el error y seguir. No bloquear el consumer.
- **Respuesta siempre:** en request-reply, el servicio responde aunque falle (con `ok: false`). El BFF no debe esperar al timeout por un error previsible.

## 8. Creación de topics (local)

Ejecutar en el contenedor de Kafka, o desde un script de `infra/scripts/`:

```bash
TOPICS="
rutas.city.list rutas.city.get rutas.city.create rutas.city.update rutas.city.delete
rutas.ruta.list rutas.ruta.get rutas.ruta.create rutas.ruta.delete
rutas.travel.list rutas.travel.get rutas.travel.create rutas.travel.update
buses.driver.list buses.driver.create
buses.bus.list buses.bus.get buses.bus.create buses.bus.change-status
bus.validate
"
for t in $TOPICS; do
  kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $t --partitions 1 --replication-factor 1
  kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $t.reply --partitions 1 --replication-factor 1
done

for t in bus.status.changed travel.created; do
  kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $t --partitions 1 --replication-factor 1
done
```

## 9. Flujos de ejemplo

**Crear un viaje (la interacción bidireccional)**
1. Front: mutation `createTravel`.
2. BFF → `rutas.travel.create` (con `kafka_correlationId` y `kafka_replyTopic`).
3. ms-rutas → `bus.validate` hacia ms-buses.
4. ms-buses → `bus.validate.reply` con `available`.
5. ms-rutas guarda el viaje (o responde con error) y contesta en `rutas.travel.create.reply`.
6. BFF consulta `buses.bus.list { ids }` para completar el campo `bus` y responde a GraphQL.

**Bus en mantención**
1. Front: mutation `changeBusStatus`.
2. BFF → `buses.bus.change-status`.
3. ms-buses guarda el cambio, responde y publica `bus.status.changed`.
4. ms-rutas consume el evento y desactiva los viajes de ese bus.

**Optimizar ruta:** no usa topics nuevos. El BFF junta datos con `rutas.ruta.list`, `rutas.travel.list` y `buses.bus.list`, y los envía al MCP por REST.