# OMniLeads CDR

Por cada llamada cursada en el sistema se deja un registro en formato SQL, con las siguientes columnas:

id  |   time   | callid  | campana_id | tipo_campana | tipo_llamada | agente_id |   event    | numero_marcado | contacto_id | bridge_wait_time | duracion_llamada | archivo_grabacion | agente_extra_id | campana_extra_id | numero_extra 

### callid
Se trata del Uniqueid de la llamada telefonica con nuestro cliente. 

### campana_id
Es el ID de la campaña por la cual se esta procesando la llamada del cliente.

### tipo_campana
En OML existen estos tipos de campaña:

* manual: id=1
* dialer: id=2
* inbound: id=3
* preview: id=4

### tipo_llamada
En OML existen estos tipos de llamadas independiente de la campaña:

* manual: id=1
* dialer: id=2
* inbound: id=3
* preview: id=4
* transferencia hacia agente: id=7
* transferencia hacia campaña: id=8

### agente_id
Es el agente que originalmente interactua con la llamada del cliente. 

### event
El nombre del evento que expone el resultado de la llamada en el sistema. Ejemplos:

* CANCEL
* BUSY
* CONGESTION
* NOANSWER
* COMPLETE-OUT
* COMPLETE-AG
* COMPLETE-CT-AG
* COMPLETE-CT-OUT
* COMPLETE-CT-CAMP
* COMPLETE-BT-AG
* COMPLETE-BT-OUT
* COMPLETE-BT-CAMP
* EXPIRE
* ABANDON
* ERROR

### numero_marcado
Es el numero de telefono del cliente, independientemente de que parte haya originado la llamada.

### contacdo_id
Es el id del contacto de OML.

### bridge_wait_time
Es el tiempo que tuvo que esperar la llamada hasta ser contactada entre cliente y agente.

### duracion_llamada
La duracion total en minutos de una llamada.

### archivo_grababacion
Es el nombre del archivo sobre el que se almacena el audio de la llamada.

### agente_extra_id
Cuando se trata de una llamada con transferencia, aqui se almacena el valor del ultimo agente de OML, que interactuo en la llamada.

### campana_extra_id
Cuando se trata de una llamada con transferencia, aqui se almacena el valor de la ultima campana que atendio la llamada.

### numero_extra_id
Cuando se trata de una llamada con transferencia, aqui se almacena el valor del ultimo numero de telefono PSTN o externo que interactuo en la llamada.


