-- Obtener la transcripción
local is_voicemail = session:getVariable("is_voicemail") or ""

-- Transfer según resultado
if is_voicemail == "true" then
  -- Transferimos al contexto 'default', extensión 'buzon'
  session:transfer("buzon", "XML", "default")
else
  -- Transferimos al contexto 'default', extensión 'humano'
  session:transfer("humano", "XML", "default")
end
