"""Estados de una estación de riego (WS), tal y como los muestra el panel de CENTRAL."""

DISPONIBLE = 'DISPONIBLE'          # VERDE: activada y a la espera de una solicitud
REGANDO = 'REGANDO'                # VERDE PARPADEANDO: suministrando agua
FUGA = 'FUGA'                      # ROJO: el sensor (o el Monitor) ha detectado una avería
FUERA_SERVICIO = 'FUERA_SERVICIO'  # NARANJA: bloqueada por orden de CENTRAL
DESCONECTADA = 'DESCONECTADA'      # GRIS: el Monitor no está conectado a CENTRAL

# Salud que el Engine responde al Monitor y que el Monitor reporta a CENTRAL
SALUD_OK = 'OK'
SALUD_KO = 'KO'

# Motivos de fin de un riego
FIN_COMPLETADO = 'COMPLETADO'      # Se alcanzó la duración pedida
FIN_PARADA_MANUAL = 'PARADA_MANUAL'  # Opción de menú en la propia estación
FIN_PARADA_CENTRAL = 'PARADA_CENTRAL'
FIN_BLOQUEO = 'BLOQUEO_CENTRAL'
FIN_FUGA = 'FUGA'
FIN_DESCONEXION = 'DESCONEXION'
