from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
import json
import asyncio
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_KEY"]
MANYCHAT_API_KEY = os.environ.get("MANYCHAT_API_KEY", "")
MODELO = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")

TABLA_CONV = "conversaciones_cpm"
TABLA_LOGS = "logs_cpm"

# Cache simple de tenants en memoria (page_id -> {tenant_id, settings}).
# Se refresca cada TENANT_TTL segundos para no pegarle a Supabase en cada mensaje.
_tenant_cache = {}
TENANT_TTL = 300  # 5 minutos


def _headers(extra: dict = None):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if extra:
        h.update(extra)
    return h


# ─────────────────────────────────────────────
# RESOLUCIÓN DE TENANT (corazón multi-tenant)
# ─────────────────────────────────────────────

async def resolver_tenant(page_id: str) -> dict:
    """Dado el manychat_page_id, devuelve {tenant_id, settings} del tenant dueño.
    Usa cache en memoria con TTL. Devuelve None si no se encuentra."""
    page_id = str(page_id).strip()
    ahora = datetime.utcnow().timestamp()

    cacheado = _tenant_cache.get(page_id)
    if cacheado and (ahora - cacheado["ts"]) < TENANT_TTL:
        return cacheado["data"]

    async with httpx.AsyncClient(timeout=15) as client:
        # 1) channel_connections: page_id -> tenant_id (+ token de ManyChat del tenant)
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/channel_connections",
            headers=_headers(),
            params={"manychat_page_id": f"eq.{page_id}",
                    "select": "tenant_id,manychat_api_token"}
        )
        conn = r.json()
        if not (isinstance(conn, list) and len(conn) > 0):
            print(f"[resolver_tenant] page_id {page_id} sin tenant en channel_connections")
            return None
        tenant_id = conn[0]["tenant_id"]
        manychat_token = conn[0].get("manychat_api_token") or MANYCHAT_API_KEY

        # 2) tenants: traer settings
        r2 = await client.get(
            f"{SUPABASE_URL}/rest/v1/tenants",
            headers=_headers(),
            params={"id": f"eq.{tenant_id}", "select": "id,name,slug,settings"}
        )
        tdata = r2.json()
        if not (isinstance(tdata, list) and len(tdata) > 0):
            print(f"[resolver_tenant] tenant {tenant_id} no encontrado")
            return None
        t = tdata[0]
        data = {
            "tenant_id": tenant_id,
            "name": t.get("name", ""),
            "slug": t.get("slug", ""),
            "settings": t.get("settings") or {},
            "manychat_token": manychat_token,
        }

    _tenant_cache[page_id] = {"ts": ahora, "data": data}
    return data


# ─────────────────────────────────────────────
# SUPABASE — conversaciones (con tenant_id)
# ─────────────────────────────────────────────

async def get_conversacion(tenant_id: str, contact_id: str) -> dict:
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/{TABLA_CONV}",
            headers=_headers(),
            params={"tenant_id": f"eq.{tenant_id}", "contact_id": f"eq.{contact_id}",
                    "select": "historial,agente_activo,tarea_pendiente,pedido_en_curso,direccion_entrega"}
        )
        data = r.json()
        if isinstance(data, list) and len(data) > 0:
            d = data[0]
            return {
                "historial": d.get("historial") or [],
                "agente_activo": d.get("agente_activo") or "none",
                "tarea": str(d.get("tarea_pendiente") or "").strip().lower(),
                "pedido": d.get("pedido_en_curso") or [],
                "direccion": d.get("direccion_entrega") or "",
            }
        return {"historial": [], "agente_activo": "none", "tarea": "", "pedido": [], "direccion": ""}


async def upsert_conversacion(tenant_id: str, contact_id: str, campos: dict):
    """Crea o actualiza la fila del contacto en este tenant."""
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/{TABLA_CONV}",
            headers=_headers(),
            params={"tenant_id": f"eq.{tenant_id}", "contact_id": f"eq.{contact_id}", "select": "id"}
        )
        existe = r.json()
        campos = dict(campos)
        campos["actualizado_en"] = datetime.utcnow().isoformat()
        if isinstance(existe, list) and len(existe) > 0:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/{TABLA_CONV}",
                headers=_headers({"Content-Type": "application/json", "Prefer": "return=minimal"}),
                params={"tenant_id": f"eq.{tenant_id}", "contact_id": f"eq.{contact_id}"},
                json=campos
            )
        else:
            campos["tenant_id"] = tenant_id
            campos["contact_id"] = contact_id
            await client.post(
                f"{SUPABASE_URL}/rest/v1/{TABLA_CONV}",
                headers=_headers({"Content-Type": "application/json", "Prefer": "return=minimal"}),
                json=campos
            )


async def guardar_log(tenant_id: str, contact_id: str, agente: str, mensaje: str, respuesta: str):
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/{TABLA_LOGS}",
                headers=_headers({"Content-Type": "application/json"}),
                json={
                    "tenant_id": tenant_id,
                    "contact_id": contact_id,
                    "agente": agente,
                    "mensaje": mensaje,
                    "respuesta": respuesta,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    except Exception as e:
        print(f"[guardar_log] error: {e}")


# ─────────────────────────────────────────────
# CATÁLOGO (Capa 2)
# ─────────────────────────────────────────────

# Cache de lista liviana por tenant (nombre+keywords+categoría). Refresca cada CAT_TTL.
_catalogo_cache = {}
CAT_TTL = 300


async def get_lista_liviana(tenant_id: str) -> list:
    """Trae catálogo liviano: name, keywords, categoría. Poco texto para inyectar al modelo."""
    ahora = datetime.utcnow().timestamp()
    cacheado = _catalogo_cache.get(tenant_id)
    if cacheado and (ahora - cacheado["ts"]) < CAT_TTL:
        return cacheado["data"]

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/products",
            headers=_headers(),
            params={"tenant_id": f"eq.{tenant_id}", "active": "eq.true",
                    "select": "name,keywords,product_categories(name)"}
        )
        data = r.json()
    lista = []
    if isinstance(data, list):
        for p in data:
            cat = ""
            pc = p.get("product_categories")
            if isinstance(pc, dict):
                cat = pc.get("name", "") or ""
            kws = p.get("keywords") or []
            lista.append({"name": p.get("name", ""), "keywords": kws, "categoria": cat})
    _catalogo_cache[tenant_id] = {"ts": ahora, "data": lista}
    return lista


def formato_lista_liviana(lista: list) -> str:
    """Texto compacto del catálogo para inyectar al asesor en el paso 1."""
    if not lista:
        return "(catálogo vacío)"
    lineas = []
    for p in lista:
        kws = ", ".join(p["keywords"]) if p["keywords"] else ""
        cat = f" [{p['categoria']}]" if p["categoria"] else ""
        extra = f" (palabras: {kws})" if kws else ""
        lineas.append(f"- {p['name']}{cat}{extra}")
    return "\n".join(lineas)


async def get_detalle_productos(tenant_id: str, nombres: list) -> list:
    """Dado nombres exactos de productos, trae su detalle completo con variantes."""
    if not nombres:
        return []
    # PostgREST: filtrar por name in (...). Escapamos comillas dobles.
    valores = ",".join('"' + n.replace('"', '') + '"' for n in nombres)
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/products",
            headers=_headers(),
            params={"tenant_id": f"eq.{tenant_id}", "active": "eq.true",
                    "name": f"in.({valores})",
                    "select": "name,ai_description,related_ids,product_variants(name,price,stock,reserved,is_default,active)"}
        )
        data = r.json()
    detalle = []
    if isinstance(data, list):
        for p in data:
            variantes = []
            for v in (p.get("product_variants") or []):
                if not v.get("active", True):
                    continue
                stock = v.get("stock") or 0
                reserved = v.get("reserved") or 0
                disponible = max(0, stock - reserved)
                variantes.append({
                    "variante": v.get("name", "Estándar"),
                    "precio": v.get("price"),
                    "disponible": disponible,
                    "is_default": v.get("is_default", False),
                })
            detalle.append({
                "name": p.get("name", ""),
                "ai_description": p.get("ai_description", "") or "",
                "variantes": variantes,
            })
    return detalle


def formato_detalle(detalle: list) -> str:
    """Texto con el detalle completo para que el asesor arme la respuesta (paso 2)."""
    if not detalle:
        return "(no se encontraron esos productos)"
    bloques = []
    for p in detalle:
        vs = []
        for v in p["variantes"]:
            disp = "SÍ hay stock" if v["disponible"] > 0 else "SIN stock"
            precio = f"${v['precio']}" if v["precio"] is not None else "precio no disponible"
            vs.append(f"  · {v['variante']}: {precio} — {disp} (disponibles: {v['disponible']})")
        vtxt = "\n".join(vs) if vs else "  (sin variantes activas)"
        bloques.append(f"PRODUCTO: {p['name']}\nDescripción: {p['ai_description']}\n{vtxt}")
    return "\n\n".join(bloques)




async def llamar_claude(system_prompt: str, mensajes: list, max_tokens: int = 700) -> str:
    for intento in range(3):
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                r = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": ANTHROPIC_KEY,
                        "anthropic-version": "2023-06-01",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": MODELO,
                        "max_tokens": max_tokens,
                        "system": [
                            {"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}
                        ],
                        "messages": mensajes
                    }
                )
            data = r.json()
            if "content" in data:
                return data["content"][0]["text"]
            print(f"[llamar_claude] intento {intento+1} status={r.status_code} resp={data}")
            if r.status_code in (429, 529, 500, 503):
                await asyncio.sleep(2 * (intento + 1))
                continue
            return None
        except Exception as e:
            print(f"[llamar_claude] excepción intento {intento+1}: {e}")
            await asyncio.sleep(2 * (intento + 1))
    return None


def _extraer_json(raw: str):
    if not raw:
        return {}
    cuerpo = raw
    if "---JSON---" in cuerpo:
        cuerpo = cuerpo.split("---JSON---", 1)[1]
    if "---FIN---" in cuerpo:
        cuerpo = cuerpo.split("---FIN---", 1)[0]
    cuerpo = cuerpo.replace("```json", "").replace("```JSON", "").replace("```", "").strip()
    try:
        return json.loads(cuerpo)
    except Exception:
        pass
    try:
        ini = cuerpo.index("{")
        fin = cuerpo.rindex("}") + 1
        return json.loads(cuerpo[ini:fin])
    except Exception:
        return {}


def parsear_respuesta(raw: str):
    texto = raw
    if "---JSON---" in raw:
        texto = raw.split("---JSON---", 1)[0].strip()
    elif "```" in raw:
        texto = raw.split("```", 1)[0].strip()
    texto = texto.replace("**", "").replace("__", "")
    return texto, _extraer_json(raw)


# ─────────────────────────────────────────────
# PROMPTS GENÉRICOS — se rellenan con la config del tenant
# ─────────────────────────────────────────────

def _ctx_tenant(cfg: dict) -> str:
    """Bloque de identidad común, armado desde tenants.settings."""
    nombre = cfg.get("bot_nombre", "el asistente")
    rubro = cfg.get("rubro", "un comercio")
    personalidad = cfg.get("personalidad", "cálido, profesional, rioplatense")
    reglas = cfg.get("reglas_extra", [])
    reglas_txt = ""
    if reglas:
        reglas_txt = "\nREGLAS PROPIAS DEL NEGOCIO:\n" + "\n".join(f"- {r}" for r in reglas)
    return (f"Sos {nombre}, el asistente virtual de {rubro}. "
            f"Tu personalidad: {personalidad}. Hablás en español rioplatense, mensajes cortos de 2-3 líneas, "
            f"sin bullets ni markdown. Sé cálido pero sobrio: evitá '¡Excelente!', '¡Genial!' y valoraciones "
            f"sobre el cliente o su consulta.{reglas_txt}")


def prompt_router(cfg: dict) -> str:
    return f"""{_ctx_tenant(cfg)}

PERO AHORA actuás como CLASIFICADOR INTERNO. NO le hablás al contacto. Leé el último mensaje y el historial reciente y decidí qué área responde. Devolvé SOLO un JSON: {{"ruta": "ASESOR"}}

Rutas:
- ASESOR: consulta sobre productos (qué hay, características, precios, disponibilidad, recomendaciones, "qué me conviene").
- PEDIDO: el contacto quiere comprar / agregar productos / armar o confirmar un pedido ("quiero 3 de esto", "agregá", "cerrá el pedido", "cuánto sería").
- CONTINUAR: responde a algo que se le venía preguntando (un dato, una cantidad, una confirmación "sí"/"dale", una dirección).
- CHARLA: saludo, cortesía, agradecimiento, sin intención concreta.
- AGENTE_HUMANO: SOLO si pide explícitamente hablar con una persona real.

REGLAS:
- Si hay TAREA EN CURSO y el contacto sigue el hilo → CONTINUAR.
- AGENTE_HUMANO solo ante pedido explícito de un humano. Nunca por las dudas.
- Ante duda: si hay tarea en curso, CONTINUAR; si no, CHARLA.

Devolvé SOLO {{"ruta": "..."}}."""


def prompt_charla(cfg: dict) -> str:
    saludo = cfg.get("saludo", "")
    extra = f'\nSi es el primer saludo, podés usar algo como: "{saludo}"' if saludo else ""
    return f"""{_ctx_tenant(cfg)}

El contacto te saludó o hace charla casual. Respondé cálido y breve. Si pregunta en qué podés ayudar, explicá que podés asesorarlo sobre los productos y tomar su pedido. NO inventes productos ni precios.{extra}

Devolvé SOLO el texto al contacto. Sin JSON, sin markdown."""


def prompt_asesor_paso1(cfg: dict, lista_txt: str) -> str:
    """Paso 1: el asesor ve la lista liviana e identifica qué productos pide el cliente."""
    return f"""{_ctx_tenant(cfg)}

Tu tarea ahora es INTERNA: identificar qué productos del catálogo pueden responder a la consulta del cliente. NO le escribís al cliente todavía.

El cliente puede pedir un producto por su nombre, por palabras parecidas, o por categoría ("qué tenés en limpieza"). Mirá el catálogo y elegí los productos relevantes (hasta 6). Si pide una categoría, elegí los de esa categoría. Si no encontrás nada parecido, devolvé lista vacía.

CATÁLOGO (nombres exactos — devolvé los nombres TAL CUAL aparecen acá):
{lista_txt}

Devolvé SOLO este JSON, sin texto adicional:
---JSON---
{{"productos": ["Nombre exacto 1", "Nombre exacto 2"]}}
---FIN---"""


def prompt_asesor_paso2(cfg: dict, detalle_txt: str) -> str:
    """Paso 2: el asesor responde al cliente usando el detalle real de los productos."""
    return f"""{_ctx_tenant(cfg)}

Tu rol: ASESORAR sobre los productos. Atendés tanto a quien no sabe qué quiere (ayudalo a elegir) como a quien ya sabe (respondé directo). Usá SOLO la información de abajo, nunca inventes productos, precios ni características.

REGLAS:
- Confirmá SIEMPRE si el producto está disponible (tenés el stock abajo). Si no hay stock, decilo y, si hay, ofrecé alternativas o variantes.
- Dá el precio SOLO si el cliente lo pregunta. Si no preguntó, no lo menciones.
- Si abajo hay varios productos, presentá las opciones de forma clara y breve.
- Si la info de abajo dice "(no se encontraron esos productos)", decí con naturalidad que no tenés ese producto y preguntá qué más necesita. No inventes.

INFORMACIÓN REAL DE LOS PRODUCTOS:
{detalle_txt}

Devolvé SOLO el texto al cliente. Sin JSON, sin markdown."""


def prompt_pedido(cfg: dict) -> str:
    # Placeholder de la Capa 3. Por ahora deriva a asesor con un mensaje suave.
    return f"""{_ctx_tenant(cfg)}

El contacto quiere hacer un pedido. En esta etapa todavía no está habilitada la toma de pedidos automática. Pedile amablemente que te diga qué productos le interesan así lo vas asesorando, y avisá que en breve vas a poder cerrar el pedido. NO inventes precios.

Devolvé SOLO el texto al contacto. Sin JSON, sin markdown."""


def prompt_agente_humano(cfg: dict) -> str:
    return f"""{_ctx_tenant(cfg)}

El contacto pidió hablar con una persona. Confirmá que ya estás avisando a alguien del comercio para que lo atienda, de forma cálida y breve.

RESPUESTA JSON OBLIGATORIA (el usuario NO la ve):
---JSON---
{{"escalar": true}}
---FIN---"""


# ─────────────────────────────────────────────
# CEREBRO
# ─────────────────────────────────────────────

RUTAS_VALIDAS = ("ASESOR", "PEDIDO", "CONTINUAR", "CHARLA", "AGENTE_HUMANO")
AGENTES_CONTENIDO = ("asesor", "pedido", "agente_humano")


async def clasificar_ruta(cfg: dict, historial: list, mensaje: str, tarea: str) -> str:
    hist = historial[-6:] if len(historial) > 6 else historial
    contexto = ""
    if tarea in AGENTES_CONTENIDO:
        contexto = f"[CONTEXTO: hay una tarea de '{tarea}' en curso. Si el contacto sigue el hilo, es CONTINUAR.]"
    contenido = f"{contexto}\nMensaje del contacto: {mensaje}".strip()
    msgs = hist + [{"role": "user", "content": contenido}]
    raw = await llamar_claude(prompt_router(cfg), msgs, max_tokens=60)
    _, jr = parsear_respuesta(raw)
    ruta = (jr.get("ruta") or "CHARLA").upper().strip()
    return ruta if ruta in RUTAS_VALIDAS else "CHARLA"


async def manejar_turno(tenant: dict, contact_id: str, mensaje: str):
    tenant_id = tenant["tenant_id"]
    cfg = tenant["settings"]

    conv = await get_conversacion(tenant_id, contact_id)
    historial = conv["historial"]
    if len(historial) > 40:
        historial = historial[-40:]
    tarea = conv["tarea"]

    ruta = await clasificar_ruta(cfg, historial, mensaje, tarea)

    if ruta == "CONTINUAR":
        agente = tarea if tarea in AGENTES_CONTENIDO else "asesor"
    elif ruta == "ASESOR":
        agente = "asesor"
    elif ruta == "PEDIDO":
        agente = "pedido"
    elif ruta == "AGENTE_HUMANO":
        agente = "agente_humano"
    else:
        agente = "charla"

    historial.append({"role": "user", "content": mensaje})

    if agente == "charla":
        raw = await llamar_claude(prompt_charla(cfg), historial, max_tokens=300)
    elif agente == "asesor":
        # Paso 1: identificar qué productos pide el cliente (lista liviana)
        lista = await get_lista_liviana(tenant_id)
        raw1 = await llamar_claude(
            prompt_asesor_paso1(cfg, formato_lista_liviana(lista)),
            historial, max_tokens=200
        )
        _, jd1 = parsear_respuesta(raw1)
        nombres = jd1.get("productos") or []
        # Paso 2: traer detalle real y responder
        detalle = await get_detalle_productos(tenant_id, nombres) if nombres else []
        raw = await llamar_claude(
            prompt_asesor_paso2(cfg, formato_detalle(detalle)),
            historial, max_tokens=700
        )
    elif agente == "pedido":
        raw = await llamar_claude(prompt_pedido(cfg), historial, max_tokens=500)
    else:  # agente_humano
        raw = await llamar_claude(prompt_agente_humano(cfg), historial, max_tokens=200)

    if not raw:
        return None, None, {}

    texto, json_data = parsear_respuesta(raw)
    historial.append({"role": "assistant", "content": texto})

    # Persistir
    nueva_tarea = agente if agente in AGENTES_CONTENIDO else ""
    await upsert_conversacion(tenant_id, contact_id, {
        "historial": historial,
        "agente_activo": agente,
        "tarea_pendiente": nueva_tarea,
    })
    await guardar_log(tenant_id, contact_id, agente, mensaje, texto)

    return agente, texto, json_data


def _respuesta_unificada(agente, texto, json_data):
    jd = json_data or {}
    return {
        "respuesta": texto,
        "mensaje": texto,
        "agente": agente,
        "escalar": bool(jd.get("escalar", False)) if agente == "agente_humano" else False,
    }


# ─────────────────────────────────────────────
# ENDPOINT
# ─────────────────────────────────────────────

@app.get("/")
async def health():
    return {"status": "CPM activo — motor multi-tenant + catálogo (Capa 1+2)"}


@app.post("/orquestador")
async def orquestador(request: Request):
    body = await request.json()
    page_id = str(body.get("page_id", "")).strip()
    contact_id = str(body.get("contact_id", "")).strip()
    mensaje = body.get("mensaje_usuario", "")

    if not page_id:
        return JSONResponse(_respuesta_unificada("charla", "Falta configurar el page_id en el request.", {}))
    if not contact_id or not mensaje:
        return JSONResponse(_respuesta_unificada("charla", "No pude procesar tu mensaje. Intentá de nuevo.", {}))

    tenant = await resolver_tenant(page_id)
    if not tenant:
        return JSONResponse(_respuesta_unificada("charla", "No encontré la configuración de este negocio. Avisá al administrador.", {}))

    agente, texto, json_data = await manejar_turno(tenant, contact_id, mensaje)
    if texto is None:
        return JSONResponse(_respuesta_unificada("charla", "Tardé más de lo esperado. ¿Podés repetir tu mensaje?", {}))
    return JSONResponse(_respuesta_unificada(agente, texto, json_data))
