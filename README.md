# Proyecto MS Pathfinder - Arquitectura de Sistemas 2026-2

Inicializar y actualizar sub-módulos, ocupar cuando se clone el repo por primera vez
```
git submodule update --init --recursive
```
Para actualizar las referencias de los sub-módulos
```
git submodule update --remote
```


## Importante
Si se trabaja en el repositorio que tiene los sub-módulos, **primero actualizar y hacer push** en el sub-módulo y **después** en el repositorio principal. 

Si se hace al revés, se perderán las referencias de los sub-módulos en el repositorio principal y tendremos que resolver conflictos.