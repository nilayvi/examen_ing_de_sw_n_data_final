# Se puede correr con docker o manual es indistinto. Se recomienda probar con docker puesto que es mas simple


## Ejecución con Docker
Es importante cuando se hace docker compose up esperar a que termine de levantar, si se hace -d no se puede ver con claridad cuando esto pasa y el docker esta hecho para levantar airflow directamente.

```
docker-compose build --no-cache
docker-compose up 
```

Una vez que termino de levantar ir a http://localhost:8080 e iniciar el dag porque aparece deshabilitado. Este comenzara a cargar todos los archivos que hay en raw. Si hay algún problema de loguin probar en modo incógniot y/o borrar cookies porque airflow deja cookies y sessiones que complican el login.

En caso de querer ingresar a la consola usar 
``` 
docker compose exec linux-env bash
```
notar que linux-env es el nombre del servicio

Para bajar docker usar: 
```
docker-compose down
```

Obs: Todos los comandos de docker fueron probados en mac y usando la version de docker compose (sin el -) pero no deberia presentar proeblema alguno.
### Scripts utiles
- var_entorno.sh: setea todas las variables de una. 
- clean_run.sh: borra todas las carpetas accesorias para tener una corrida limpia, es equivalente a clonar el repo de nuevo pero no borra el entorno virtual


### Bugs del entorno  y el código entregado
- En mac hay que setear OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES como variable porque airflow lo usa los subprocesos que no son "fork safe",. 
Al usar subprocesos para llamar a dbt se importan librerias nativas de duckdb, dbt compiladas con Objetive-C que no son fork-safe. 
- El código parece ser para una version vieja de airflow que no soporta las sugerencias que se hicieron para pasar los argumentos, por lo tanto se modifica todo el código.
- En mac para correr el dag hubo que modificar /etc/host agregando la entrada 127.0.0.1   joses-macbook-pro.local
- ds_nodash ya no está disponible en el contexto de Jinja por defecto
Se recomienda usar logical_date que es un objeto datetime que representa la fecha lógica de ejecución
Usamos .strftime('%Y%m%d') para obtener el formato sin guiones (ej: 20251201)
-  No se pudieron usar las recomendaciones porque no son compatibles con las versiones de Airflow usadas en este entorno de evaluación
- Airflow guarda cookies, por lo tanto al iniciar el entorno dependendiendo de que tipo de configuracion se tenga el navegador puede mostrar login incorrecto, probar loguearse de forma incógnita para validar si son las cookies o valores en la session, de ser asi, eliminarlos o trabajar de modo incógnito.

### Mejoras Ralizadas
- Se agregan mas archivos raw para que pueda correr el proceso con mas de un dia scheduleado y no sea necesario correrlo por consola en un día determinado
- Se implementa que la lectura de los archivos apendee datos. No se le encuentra sentido a que solo deje la última fecha en el wharehouse asi que se carga por customer_id+_fecha
- Se soluciona el problema de la falla cuando no hay archivos generando uno vacio
### Mejoras.y bugs a resolver
- Usar postgres u otra base de datos mas robusta
- Mejorar el manejo de fechas para que tome el uso horario correspondiente
- Cuando los raw estan con el mismo id el proceso rompe. Se podria validar tal caso
- A veces los procesos se quedan "colgados" si se les da reniciar avanzan, es un bug conocido pero habria que solucionarlo


### Corridas
- Se verifica idempotencia
- El dag arranca apagado hay que prenderlo y comienza a levantar todos los archivos
- se puede correr una fecha manualemente con el comando de abajo
```airflow dags trigger medallion_pipeline --run-id manual_$(date +%s) ```