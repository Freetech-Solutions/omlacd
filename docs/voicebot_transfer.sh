
1) El voicebot recibe la llamada. En los SIP Header viaja la informacion de la llamada, el id de la llamada, el id del contacto  y el id de la campaña.

Record-Route: <sip:66.97.33.29;lr=on>
Via: SIP/2.0/UDP 66.97.33.29;branch=z9hG4bK6d2c.2317293a21dea7ed62f07bfa982690ed.0
Via: SIP/2.0/UDP 10.22.72.43:5060;received=10.22.72.43;rport=5060;branch=z9hG4bKPj7baf7afd-6213-4db5-bc57-19599487b200
From: <sip:01177660020@10.22.72.43>;tag=853690e2-ff12-4a72-aae0-8a84f5dc1a96
To: <sip:1006@pstn.sephir.tech>
Contact: <sip:01177660020@10.22.72.43:5060;alias=10.22.72.43~5060~1>
Call-ID: 423a471a-3bce-49d6-880c-059361fa8387
CSeq: 14775 INVITE
Allow: OPTIONS, REGISTER, SUBSCRIBE, NOTIFY, PUBLISH, INVITE, ACK, BYE, CANCEL, UPDATE, PRACK, MESSAGE, INFO, REFER
Supported: 100rel, timer, replaces, norefersub, histinfo
Session-Expires: 1800
Min-SE: 90
.........
X-Verloop-callID: 1767205830.0
X-Verloop-customerID: 26
X-Verloop-CampID: 11
........

Una vez logueados en OML, podemos consultar las calificaciones disponibles para esta campaña.

# LOGIN voicebot on OML
curl -k -X POST "https://konecta.sephir.tech/api/v1/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=ag1&password=098098ZZZ"

# LIST CAMP DISPOSITION
curl -k -X GET "https://konecta.sephir.tech/api/v1/campaign/11/dispositionOptions/" \
  -H "Authorization: Bearer e18c3e6f8ea3efe02cc867f52fae744a8a2ac7a4"


Antes de transferir la llamada, debemos crear la calificacion para el contacto.
# CREATE DISPOSITION idContact
curl -k -X POST "https://konecta.sephir.tech/api/v1/disposition/" \
  -H "Authorization: Bearer 2ef714e0c81c50682ee7093d664bf385aeccc451" \
  -H "Content-Type: application/json" \
  -d '{
  "idContact": 26,
  "idDispositionOption": 327,
  "comments": "Customer interested on Movistar Add-ons"
}'

Finalmente, transferimos la llamada a la campaña Django EP.

# TRANSFER TO CAMPAIGN Django EP
curl -k -X POST https://konecta.sephir.tech/api/v1/transfer/blind-campaign-agent/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer 2ef714e0c81c50682ee7093d664bf385aeccc451" \
  -d '{
    "call_id": "1767626587.126",
    "campaign_id": "11",
    "agent_id": "3"
  }'


# Obtener token y guardarlo en variable
TOKEN=$(curl -s -X POST https://konecta.sephir.tech/api/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"voicebot","password":"675vERLOOP#"}' | jq -r '.token')

curl -k -X POST https://konecta.sephir.tech/api/v1/webhook/verloop/ \
  --header 'Authorization: Bearer c1676b211ca553598731dd4a623ebf3babda75a8' \
  --header 'Content-Type: application/json' \
  --data '{
    "X-Verloop-customerID": "6",
    "X-Verloop-CampID": "11",
    "X-Verloop-Disposition": "32",
        "analysis": {
            "user_defined": {
                "issue": "NA",
                "call_summary": "The customer was contacted by Aanya...",
                "callback_slot": "NA",
                "work_type": "NA"
            }
        },
        "channel": "voice",
        "intent_detected": "customer_interested",
        "phone": "91xxxxxxxxx",
        "trigger_type": "call_analysis"
    
}'  
# ------------------------------------------------------------------------------

# TRANSFER TO Human AGENTS
curl -k -X POST http://127.0.0.1:1441/api/transfer/blind-campaign-agent \
     -H "Content-Type: application/json" \
     -d '{
           "call_id": "1767206550.4",
           "campaign_id": "11",
           "agent_id": "3"
         }'

curl -X POST http://konecta.sephir.tech:1441/api/transfer/blind-campaign-agent \
  -H "Content-Type: application/json" \
  -d '{
    "call_id": "1767206550.4",
    "campaign_id": "11",
    "agent_id": "3"
  }'

# Only on voicebot SIP register scenary
# READY
curl -k -X POST \
  'https://127.0.0.1/api/v1/asterisk_ready/' \
  -H 'Authorization: Bearer 80590e0ce901c56c1443acdd7acf565312f4ea38'

# PAUSE
curl -k -X POST \
  'https://127.0.0.1/api/v1/asterisk_pause/' \
  -H 'Authorization: Bearer 1cd27d80ffd7d16e304fe558c39743233619dce0' \
  -H 'Content-Type: application/json' \
  -d '{"pause_id": 1}'

# UNPAUSE
curl -k -X POST \
  'https://127.0.0.1/api/v1/asterisk_unpause/' \
  -H 'Authorization: Bearer 1cd27d80ffd7d16e304fe558c39743233619dce0' \
  -H 'Content-Type: application/json' \
  -d '{"pause_id": 1}'

# TRANSFER TO AGENTS
curl -k -X POST http://127.0.0.1:5000/api/transfer/blind-campaign-agent \
     -H "Content-Type: application/json" \
     -d '{
           "call_id": "1766529313.20",
           "campaign_id": "10",
           "agent_id": "1001"
         }'

# TRANSFER TO CAMPAIGN
curl -k -X POST http://konecta.sephir.tech:1441/api/transfer/blind-campaign \
     -H "Content-Type: application/json" \
     -d '{
           "call_id": "1767178876.32",
           "target_campaign_id": "25",
           "agent_id": "3"
         }'