# Ejemplos de integración voicebot → OML (transferencias y calificaciones).
# Reemplazar placeholders antes de ejecutar. No commitear credenciales reales.

# 1) El voicebot recibe la llamada. En los SIP Header viaja la informacion de la
# llamada, el id de la llamada, el id del contacto y el id de la campaña.
#
# Record-Route: <sip:203.0.113.10;lr=on>
# Via: SIP/2.0/UDP 203.0.113.10;branch=z9hG4bK...
# From: <sip:01177660020@10.0.0.43>;tag=853690e2-ff12-4a72-aae0-8a84f5dc1a96
# To: <sip:1006@pstn.example.com>
# X-Verloop-callID: 1767205830.0
# X-Verloop-customerID: 26
# X-Verloop-CampID: 11

OML_HOST="${OML_HOST:-oml.example.com}"
OML_USER="${OML_USER:-voicebot_user}"
OML_PASSWORD="${OML_PASSWORD:-<password>}"
OML_API_TOKEN="${OML_API_TOKEN:-<api_token>}"

# LOGIN voicebot on OML
curl -k -X POST "https://${OML_HOST}/api/v1/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=${OML_USER}&password=${OML_PASSWORD}"

# LIST CAMP DISPOSITION
curl -k -X GET "https://${OML_HOST}/api/v1/campaign/11/dispositionOptions/" \
  -H "Authorization: Bearer ${OML_API_TOKEN}"

# CREATE DISPOSITION idContact
curl -k -X POST "https://${OML_HOST}/api/v1/disposition/" \
  -H "Authorization: Bearer ${OML_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
  "idContact": 26,
  "idDispositionOption": 327,
  "comments": "Customer interested on Movistar Add-ons"
}'

# TRANSFER TO CAMPAIGN Django EP
curl -k -X POST "https://${OML_HOST}/api/v1/transfer/blind-campaign-agent/" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${OML_API_TOKEN}" \
  -d '{
    "call_id": "1767626587.126",
    "campaign_id": "11",
    "agent_id": "3"
  }'

# Obtener token y guardarlo en variable
TOKEN=$(curl -s -X POST "https://${OML_HOST}/api/v1/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${OML_USER}\",\"password\":\"${OML_PASSWORD}\"}" | jq -r '.token')

curl -k -X POST "https://${OML_HOST}/api/v1/webhook/verloop/" \
  --header "Authorization: Bearer ${TOKEN}" \
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
# TRANSFER TO Human AGENTS (ARI / servicio local)
curl -k -X POST http://127.0.0.1:1441/api/transfer/blind-campaign-agent \
  -H "Content-Type: application/json" \
  -d '{
    "call_id": "1767206550.4",
    "campaign_id": "11",
    "agent_id": "3"
  }'

curl -X POST "http://${OML_HOST}:1441/api/transfer/blind-campaign-agent" \
  -H "Content-Type: application/json" \
  -d '{
    "call_id": "1767206550.4",
    "campaign_id": "11",
    "agent_id": "3"
  }'

# Only on voicebot SIP register scenario
# READY
curl -k -X POST "https://127.0.0.1/api/v1/asterisk_ready/" \
  -H "Authorization: Bearer ${OML_API_TOKEN}"

# PAUSE
curl -k -X POST "https://127.0.0.1/api/v1/asterisk_pause/" \
  -H "Authorization: Bearer ${OML_API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"pause_id": 1}'

# UNPAUSE
curl -k -X POST "https://127.0.0.1/api/v1/asterisk_unpause/" \
  -H "Authorization: Bearer ${OML_API_TOKEN}" \
  -H "Content-Type: application/json" \
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
curl -k -X POST "http://${OML_HOST}:1441/api/transfer/blind-campaign" \
  -H "Content-Type: application/json" \
  -d '{
    "call_id": "1767178876.32",
    "target_campaign_id": "25",
    "agent_id": "3"
  }'
