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
OPENAI_KEY = os.environ.get("OPENAI_KEY", "")
MODELO = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
CPM_API_URL = os.environ.get("CPM_API_URL", "https://cpm-nexent.vercel.app/api/agent")
# Key para autenticar contra el CPM. Si no se define, usa SUPABASE_KEY (mismo secret).
CPM_API_KEY = os.environ.get("CPM_API_KEY", os.environ.get("SUPABASE_KEY", ""))

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
                    "select": "historial,agente_activo,tarea_pendiente,pedido_en_curso,direccion_entrega,ultimo_pedido_fecha,ultimo_pedido_num"}
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
                "ultimo_pedido_fecha": d.get("ultimo_pedido_fecha") or "",
                "ultimo_pedido_num": d.get("ultimo_pedido_num") or "",
            }
        return {"historial": [], "agente_activo": "none", "tarea": "", "pedido": [], "direccion": "",
                "ultimo_pedido_fecha": "", "ultimo_pedido_num": ""}


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

# Cache de catálogo del CPM por tenant. TTL corto porque promos/stock cambian.
_catalogo_cache = {}
CAT_TTL = 60


async def cpm_get_catalogo(tenant_id: str) -> list:
    """GET /catalog del CPM — fuente de verdad: precios, stock, promos, fraccionamiento.
       Los productos con promo activa y cupo vienen primero."""
    ahora = datetime.utcnow().timestamp()
    cacheado = _catalogo_cache.get(tenant_id)
    if cacheado and (ahora - cacheado["ts"]) < CAT_TTL:
        return cacheado["data"]
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(
                f"{CPM_API_URL}/catalog",
                headers=_headers_cpm(),
                params={"tenant_id": tenant_id},
            )
        if r.status_code != 200:
            print(f"[cpm_get_catalogo] status={r.status_code} resp={r.text[:200]}")
            return cacheado["data"] if cacheado else []
        data = r.json()
        productos = data.get("products", data) if isinstance(data, dict) else data
        if not isinstance(productos, list):
            productos = []
        _catalogo_cache[tenant_id] = {"ts": ahora, "data": productos}
        return productos
    except Exception as e:
        print(f"[cpm_get_catalogo] excepción: {e}")
        return cacheado["data"] if cacheado else []


async def get_lista_liviana(tenant_id: str) -> list:
    """Catálogo desde el CPM, adaptado al formato liviano que usan los prompts.
       Ahora incluye precios, promo y fraccionamiento."""
    productos = await cpm_get_catalogo(tenant_id)
    lista = []
    for p in productos:
        lista.append({
            "name": p.get("name", ""),
            "keywords": p.get("keywords") or [],
            "categoria": p.get("category", "") or "",
            "precio_bulto": p.get("precio_bulto"),
            "precio_unidad": p.get("precio_unidad"),
            "stock_bultos": p.get("stock_disponible_bultos"),
            "stock_unidades": p.get("stock_disponible_unidades"),
            "promo": p.get("promo"),
            "fraccionada": p.get("venta_fraccionada") or {},
        })
    return lista


def formato_lista_liviana(lista: list) -> str:
    """Texto compacto del catálogo con precios, promos y fraccionamiento."""
    if not lista:
        return "(catálogo vacío)"
    lineas = []
    for p in lista:
        cat = f" [{p.get('categoria')}]" if p.get("categoria") else ""
        partes = [f"- {p['name']}{cat}"]
        pb = p.get("precio_bulto")
        if pb:
            partes.append(f"bulto ${pb:,.0f}")
        promo = p.get("promo")
        if isinstance(promo, dict) and promo.get("activa") and (promo.get("disponibles_en_promo") or 0) > 0:
            partes.append(f"🔥 PROMO {promo.get('descuento_pct')}% OFF: ${promo.get('precio_promo'):,.0f} el bulto"
                          f" (quedan {promo.get('disponibles_en_promo')} en promo)")
        fr = p.get("fraccionada") or {}
        pu = p.get("precio_unidad")
        if fr.get("permite_unidad") and pu:
            partes.append(f"unidad suelta ${pu:,.0f} ({fr.get('unidades_por_bulto')} un/bulto)")
        sb = p.get("stock_bultos")
        if sb is not None:
            partes.append(f"stock: {sb} bultos")
        lineas.append(" | ".join(partes))
    return "\n".join(lineas)


async def get_categorias_con_destacados(tenant_id: str, por_categoria: int = 3) -> str:
    """Para consultas amplias: categorías con destacados, desde el catálogo del CPM.
       Las promos activas se destacan al inicio."""
    productos = await cpm_get_catalogo(tenant_id)
    if not productos:
        return "(catálogo vacío)"
    # Promos primero (el CPM ya las ordena al inicio de la lista)
    promos = []
    for p in productos:
        promo = p.get("promo")
        if isinstance(promo, dict) and promo.get("activa") and (promo.get("disponibles_en_promo") or 0) > 0:
            promos.append(f"  🔥 {p.get('name')} — {promo.get('descuento_pct')}% OFF: ${promo.get('precio_promo'):,.0f} el bulto")
    # Agrupar por categoría
    cats = {}
    for p in productos:
        cat = p.get("category") or "Otros"
        cats.setdefault(cat, []).append(p.get("name", ""))
    bloques = []
    if promos:
        bloques.append("OFERTAS ACTIVAS (mencionalas de entrada):\n" + "\n".join(promos))
    for cat, prods in cats.items():
        destacados = prods[:por_categoria]
        lista = "\n".join(f"  • {n}" for n in destacados)
        extra = f"\n  ...y {len(prods) - por_categoria} más" if len(prods) > por_categoria else ""
        bloques.append(f"CATEGORÍA: {cat} ({len(prods)} productos)\n{lista}{extra}")
    return "\n\n".join(bloques)


def es_consulta_amplia(mensaje: str) -> bool:
    """Detecta si el cliente pregunta por el catálogo en general, no un producto puntual."""
    m = mensaje.lower()
    señales = ["que tenes", "qué tenés", "que hay", "qué hay", "catalogo", "catálogo",
               "que venden", "qué venden", "que productos", "qué productos", "todo lo que",
               "que mas hay", "qué más hay", "opciones", "lista de", "mostrame todo", "que ofrecen"]
    return any(s in m for s in señales)


async def get_detalle_productos(tenant_id: str, nombres: list) -> list:
    """Descripciones editoriales desde Supabase + precios/promos desde el catálogo CPM
       (una sola fuente de verdad para precios)."""
    if not nombres:
        return []
    valores = ",".join('"' + n.replace('"', '') + '"' for n in nombres)
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/products",
            headers=_headers(),
            params={"tenant_id": f"eq.{tenant_id}", "active": "eq.true",
                    "name": f"in.({valores})",
                    "select": "name,ai_description"}
        )
        data = r.json()
    # Precios y promos: del catálogo CPM
    catalogo = await cpm_get_catalogo(tenant_id)
    cat_por_nombre = {_norm_nombre(p.get("name", "")): p for p in catalogo}
    detalle = []
    if isinstance(data, list):
        for p in data:
            nombre = p.get("name", "")
            cpm_p = cat_por_nombre.get(_norm_nombre(nombre), {})
            promo = cpm_p.get("promo")
            promo_ok = isinstance(promo, dict) and promo.get("activa") and (promo.get("disponibles_en_promo") or 0) > 0
            fr = cpm_p.get("venta_fraccionada") or {}
            detalle.append({
                "name": nombre,
                "ai_description": p.get("ai_description", "") or "",
                "precio_bulto": cpm_p.get("precio_bulto"),
                "precio_unidad": cpm_p.get("precio_unidad") if fr.get("permite_unidad") else None,
                "stock_bultos": cpm_p.get("stock_disponible_bultos"),
                "promo": {"descuento_pct": promo.get("descuento_pct"),
                          "precio_promo": promo.get("precio_promo"),
                          "disponibles": promo.get("disponibles_en_promo")} if promo_ok else None,
            })
    return detalle


def formato_detalle(detalle: list) -> str:
    """Texto con el detalle completo para que el asesor arme la respuesta (paso 2)."""
    if not detalle:
        return "(no se encontraron esos productos)"
    bloques = []
    for p in detalle:
        lineas = [f"PRODUCTO: {p['name']}", f"Descripción: {p['ai_description']}"]
        pb = p.get("precio_bulto")
        sb = p.get("stock_bultos")
        if pb:
            stock_txt = f" — stock: {sb} bultos" if sb is not None else ""
            lineas.append(f"  · Bulto: ${pb:,.0f}{stock_txt}")
        promo = p.get("promo")
        if promo:
            lineas.append(f"  · 🔥 EN PROMO: {promo['descuento_pct']}% OFF → ${promo['precio_promo']:,.0f} el bulto "
                          f"(quedan {promo['disponibles']} en promo). OFRECELA de entrada.")
        pu = p.get("precio_unidad")
        if pu:
            lineas.append(f"  · Unidad suelta: ${pu:,.0f} (también se vende fraccionado)")
        bloques.append("\n".join(lineas))
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
- ASESOR: consulta sobre productos cuando NO hay un pedido en curso (qué hay, características, precios, disponibilidad, recomendaciones, "qué me conviene").
- PEDIDO: el contacto quiere comprar / agregar productos / armar o confirmar un pedido ("quiero 3 de esto", "agregá", "cerrá el pedido"). TAMBIÉN cae acá CUALQUIER pregunta cuando hay un pedido en curso (carrito armándose): precio, total, "cuánto es", "cómo queda el pedido", resumen, sacar o cambiar items. Si hay pedido en curso, quedate en PEDIDO.
- GESTION: el contacto pregunta por un pedido YA CONFIRMADO/registrado (no el carrito que se está armando). Señales: "cómo va mi pedido", "estado de mi pedido", "el pedido N° 1015", "ya salió?", "cuándo llega", "cancelá mi pedido", "quiero modificar el pedido que hice", "sacá un producto del pedido que ya confirmé". Es sobre un pedido pasado, no el actual en armado.
- CONTINUAR: responde a algo que se le venía preguntando (un dato, una cantidad, una confirmación "sí"/"dale", una dirección).
- CHARLA: saludo, cortesía, agradecimiento, sin intención concreta.
- AGENTE_HUMANO: SOLO si pide explícitamente hablar con una persona real.

REGLAS:
- Si hay un PEDIDO EN CURSO (carrito con productos o tarea de pedido), las preguntas sobre precio, total o el estado del pedido van a PEDIDO, NO a ASESOR. El agente de pedido tiene los precios y el carrito.
- DISTINCIÓN CLAVE PEDIDO vs GESTION: PEDIDO = el carrito que se está armando ahora. GESTION = un pedido que YA se confirmó antes (pregunta por su estado, quiere cancelarlo o modificarlo).
- GESTIÓN EN CURSO (MÁXIMA PRIORIDAD): si en el CONTEXTO de abajo dice que hay una GESTIÓN DE PEDIDO en curso, entonces TODO lo que siga sobre ese pedido —agregar productos, quitar, cambiar cantidad, preguntar el total o el precio, "cómo queda", confirmaciones ("sí", "dale")— es CONTINUAR (sigue en gestión). NO cambies a PEDIDO solo porque el cliente dice "agregá X": si venías gestionando un pedido confirmado, ese "agregá" es sobre ESE pedido, no un carrito nuevo. Solo salí de gestión si el cliente claramente arranca algo no relacionado (un pedido nuevo explícito, otra consulta).
- PRIORIDAD DE GESTION: si el cliente hace referencia a un pedido ANTERIOR / YA HECHO / YA CONFIRMADO, o menciona un NÚMERO de pedido (ej. "el 1016", "pedido N° 1015"), o pide "sumá/agregá esto AL PEDIDO ANTERIOR / al que hice", es GESTION — AUNQUE haya un carrito armándose. La mención a un pedido previo gana sobre el carrito en curso.
- Si NO hay gestión en curso NI menciona un pedido previo, y solo está sumando productos al carrito actual, es PEDIDO.
- AGENTE_HUMANO solo ante pedido explícito de un humano. Nunca por las dudas.
- Ante duda: si hay gestión en curso, quedate en GESTION (CONTINUAR); si hay carrito y no refiere a pedido previo, PEDIDO; si no, CHARLA.

Devolvé SOLO {{"ruta": "..."}}."""


def prompt_charla(cfg: dict) -> str:
    saludo = cfg.get("saludo", "")
    extra = f'\nSi es el primer saludo, podés usar algo como: "{saludo}"' if saludo else ""
    return f"""{_ctx_tenant(cfg)}

El contacto te saludó o hace charla casual. Respondé cálido y breve. Si pregunta en qué podés ayudar, explicá que podés asesorarlo sobre los productos y tomar su pedido. NO inventes productos ni precios.{extra}

Devolvé SOLO el texto al contacto. Sin JSON, sin markdown."""


def prompt_asesor_catalogo(cfg: dict, cats_txt: str) -> str:
    """Para consultas amplias: presenta categorías + destacados e invita a elegir."""
    return f"""{_ctx_tenant(cfg)}

El cliente pregunta qué hay en el catálogo (consulta amplia, no un producto puntual). Presentale las CATEGORÍAS disponibles con algunos productos destacados de cada una, de forma breve y ordenada, e invitalo a que te diga qué categoría o producto le interesa para darle más detalle. No tires los precios todavía (solo si pregunta). No inventes nada fuera de la lista.

CATÁLOGO POR CATEGORÍAS:
{cats_txt}

Devolvé SOLO el texto al cliente. Sin JSON, sin markdown."""


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


def prompt_pedido(cfg: dict, lista_txt: str, carrito_txt: str) -> str:
    return f"""{_ctx_tenant(cfg)}

Tu rol ahora: TOMAR EL PEDIDO. Atendé con naturalidad e incitá sutilmente a sumar productos (en vez de "¿con eso estaría?", preguntá "¿qué más te llevás?").

PROMOS Y VENTA FRACCIONADA (NUEVO — leer con atención):
- El catálogo de abajo marca las PROMOS activas (🔥). OFRECELAS ESPONTÁNEAMENTE: al mencionar o sugerir un producto en promo, decí el descuento y el precio promo ("está con 20% off, te queda a $67.200 el bulto en vez de $84.000"). Si el cliente lo agrega, confirmá mencionando el descuento. Si quedan pocas en promo, podés usar urgencia real. NUNCA calcules descuentos vos: usá el % y el precio tal como figuran.
- La promo aplica SOLO comprando el bulto, nunca por unidad suelta.
- Algunos productos se venden también POR UNIDAD SUELTA (figura "unidad suelta $X" en el catálogo). Si el cliente pide poca cantidad o duda, ofrecé las dos opciones ("por unidad a $7.778 o el bulto de 12 a $84.000, que conviene más"). Si el cliente pide "un [producto]" y ese producto es fraccionable, ACLARÁ si quiere unidad o bulto antes de agregar. Si NO es fraccionable, se vende solo por bulto: no ofrezcas unidad y si la piden, aclaralo amablemente.
- Upsell: si pide varias unidades sueltas y le conviene el bulto, sugerilo con los números reales.

CÓMO TRABAJÁS:
- Identificá qué productos quiere agregar el cliente, usando los nombres EXACTOS del catálogo de abajo.
- Si el cliente nombra un producto que podés identificar sin ambigüedad en el catálogo (aunque no esté escrito idéntico, ej. "limpiador de piso marina 150ml" → "Bulto Limpiador de Pisos Smart Marina 150ml"), AGREGALO DIRECTO con accion "agregar". NO preguntes de más ni muestres el pedido sin actualizarlo. Si no aclara cantidad, asumí 1 bulto y aclaralo en el texto.
- Preguntá SOLO si hay ambigüedad REAL que te impide elegir el producto: falta la fragancia entre varias opciones, falta el formato/ml y hay varios, o el producto es fraccionable y no sabés si quiere unidad o bulto. Si el cliente ya dio esos datos, no vuelvas a preguntar.
- NO agregues un producto que ya está en el carrito de nuevo. Si el cliente corrige la cantidad ("quería 1, no 2"), usá accion "reemplazar" para fijar la cantidad correcta, no "agregar".
- REGLA DE ORO ANTI-DUPLICADO: mirá el CARRITO ACTUAL de abajo. Si un producto YA figura ahí con la cantidad que el cliente quiere, NO lo vuelvas a incluir en "items" con accion "agregar". Solo usá "agregar" para productos NUEVOS o para CANTIDAD ADICIONAL que el cliente pide explícitamente ("sumale 2 más"). Repetir un "agregar" con lo que ya está DUPLICA el pedido y es un error grave.
- Cuando el cliente solo REAFIRMA o ACEPTA ("sí", "dale", "confirmo", "correcto", "está bien") SIN nombrar productos ni cantidades nuevas, NUNCA es "agregar". Es "confirmar" (si venías pidiendo confirmación del pedido) o "nada" (si respondía otra cosa). JAMÁS devuelvas "agregar" con los mismos productos del carrito solo porque el cliente dijo "dale".
- PRECIOS Y ESTADO DEL CARRITO: los datos del CARRITO ACTUAL de abajo son la VERDAD. Si el carrito tiene productos, TENÉS los precios y el total: informalos. Está PROHIBIDO decir "el carrito está vacío", "no tengo los precios" o "no me corresponde tu pedido" cuando el CARRITO ACTUAL de abajo tiene ítems. Si tiene ítems, esos SON el pedido del cliente: trabajá con ellos, no los cuestiones.

CONFIRMACIÓN (importante):
- Cuando el cliente quiera cerrar, pedí confirmación EXPLÍCITA: "¿Confirmás el pedido?".
- Marcá accion "confirmar" SOLO si el cliente confirma de forma clara: "confirmo", "sí, cerrá", "dale cerralo", "está bien cerrá". 
- Si el cliente dice algo ambiguo como "si" mientras pregunta otra cosa (ej. "si, cuánto es?"), NO es una confirmación: respondé su pregunta y volvé a pedir confirmación explícita. Ante la duda, NO confirmes.

FORMATO DE TU RESPUESTA (CRÍTICO — leer con atención):
- Está TERMINANTEMENTE PROHIBIDO escribir tablas, listas de productos, o el detalle del pedido (con "|", con guiones, o en cualquier formato) en tu texto. JAMÁS. El sistema muestra AUTOMÁTICAMENTE una imagen con el resumen. Si vos escribís la tabla, se duplica y se ve mal.
- Cuando agregás productos o te piden el resumen/cotización, tu texto debe ser CORTO y sin detalle. Ejemplo: "¡Listo! Acá te dejo el resumen 👇 ¿Confirmás o sumás algo más?". La imagen muestra los productos, no vos.
- Si el CARRITO ACTUAL está vacío o dice "(vacío)", significa que NO hay pedido en curso (puede que ya se haya confirmado). En ese caso NO inventes ni recuerdes productos de antes: decí que no hay un pedido activo y preguntá qué querés pedir. NUNCA armes una tabla con datos que no están en el carrito actual.

CATÁLOGO (nombres exactos):
{lista_txt}

CARRITO ACTUAL (con precios, solo para tu referencia — NO lo copies en el texto):
{carrito_txt}

Respondé SIEMPRE con texto al cliente + este JSON al final (el cliente NO ve el JSON):
---JSON---
{{"accion": "agregar|reemplazar|nada|confirmar", "items": [{{"producto": "Nombre exacto del catálogo", "cantidad": 1, "unidad": "bulto"}}]}}
---FIN---

REGLAS DEL JSON (CRÍTICO):
- El bloque ---JSON---...---FIN--- es OBLIGATORIO en CADA respuesta. NUNCA lo omitas, aunque solo estés charlando.
- Si venías preguntando una cantidad ("¿cuántos bultos?") y el cliente responde un número o confirma, ESE turno DEBE llevar accion "agregar" con el producto y la cantidad. No respondas "listo, anotado" con items vacío: si dijiste que anotabas, el JSON tiene que reflejar el agregado real.
- accion "agregar": sumar productos NUEVOS al carrito.
- accion "reemplazar": corregir la cantidad de un producto que YA está en el carrito.
- accion "resumen": cuando el cliente pide VER el pedido/resumen/cotización sin cambiar nada ("pasame el resumen", "cómo queda", "cuánto es el total", "mostrame el pedido").
- accion "nada": SOLO cuando de verdad no cambiás el carrito y no piden ver el resumen (una duda puntual, un saludo).
- accion "confirmar": SOLO ante confirmación explícita del cliente.
- "unidad": "bulto" (default) o "unidad" si el cliente elige comprar unidades sueltas de un producto fraccionable. La cantidad entonces es en UNIDADES, no bultos.
- Usá SIEMPRE el nombre exacto del catálogo."""


def prompt_gestion(cfg: dict, pedidos_txt: str, lista_txt: str) -> str:
    return f"""{_ctx_tenant(cfg)}

Tu rol ahora: GESTIONAR PEDIDOS YA CONFIRMADOS. El cliente pregunta por el estado de un pedido que ya hizo, o quiere modificarlo (agregar, quitar o cambiar cantidad de productos) o cancelarlo. NO estás armando un carrito nuevo desde cero.

PEDIDOS DEL CLIENTE (datos reales del sistema — son la VERDAD, no inventes nada):
{pedidos_txt}

CATÁLOGO (nombres exactos — usalos TAL CUAL para agregar productos nuevos):
{lista_txt}

PROMOS (importante): si un producto del catálogo figura con 🔥 PROMO y el cliente lo agrega, mencioná el descuento ("está con 20% off"). La promo aplica SOLO por bulto. Si el cliente aumenta la cantidad de un renglón que ya está [EN PROMO], puede fallar por cupo — si el sistema lo rechaza, avisá cuántas quedan y ofrecé el resto a precio normal.

ESTADOS Y CÓMO NOMBRARLOS AL CLIENTE (NUNCA escribas el nombre técnico con guión bajo, usá la versión natural):
- pendiente → "pendiente, todavía no lo empezó a preparar el local"
- en_preparacion → "en preparación, el local ya lo está armando"
- para_enviar → "listo para salir a reparto"
- en_entrega → "en camino"
- entregado → "entregado"
- cancelado → "cancelado"

REGLAS DE NEGOCIO (respetalas SIEMPRE):
- MODIFICAR (agregar/quitar/cambiar cantidad): permitido SOLO si el pedido está en estado pendiente. Si está en preparación o más avanzado, NO se puede: explicale con calidez que el local ya lo está preparando y ofrecele hacer un pedido nuevo con lo que quiera sumar.
- CANCELAR: permitido si el pedido está pendiente, en preparación o listo para salir. NO se puede si ya está en camino o entregado: avisale que hable con el equipo.
- CONSULTAR estado: siempre se puede.
- PRECIOS Y TOTAL: en la lista de PEDIDOS DEL CLIENTE de arriba tenés el precio de cada producto y el TOTAL de cada pedido. Si el cliente pregunta el precio o el total, RESPONDÉSELO con esos datos. NUNCA digas "no tengo los precios" ni "consultá con el equipo por el total": los datos están arriba, usalos.

QUÉ PEDIDO:
- Si el cliente menciona un número (ej. "pedido 1015"), buscá ese en la lista de arriba.
- Si no menciona número, asumí el más reciente de la lista.
- Si no hay pedidos, decile con naturalidad que no encontrás pedidos asociados y ofrecé tomar uno nuevo.

FORMATO: mensajes cortos, cálidos, sin markdown ni tablas ni guiones bajos. Respondé SIEMPRE con texto al cliente + este JSON al final (el cliente NO lo ve):
---JSON---
{{"accion_gestion": "consultar|modificar|cancelar|nada", "order_number": "1015 o vacío", "cambios": [{{"producto": "Nombre exacto del catálogo", "cantidad": 2, "operacion": "agregar|cambiar|quitar"}}]}}
---FIN---

REGLAS DEL JSON:
- accion_gestion "consultar": el cliente solo quiere saber el estado. cambios vacío.
- accion_gestion "cancelar": el cliente quiere cancelar. Poné el order_number.
- accion_gestion "modificar": SOLO si el cliente pide un cambio concreto Y el pedido está pendiente. Poné order_number y cambios.
- accion_gestion "nada": charla, o cuando el estado NO permite lo que pide (ahí explicás por qué en el texto, sin intentar la acción).
- En "cambios", cada ítem lleva "operacion":
  · "agregar" = producto NUEVO que no estaba en el pedido (cantidad = cuántos bultos sumar).
  · "cambiar" = producto que YA está, nueva cantidad final.
  · "quitar" = sacar un producto que está en el pedido (cantidad se ignora).
- Usá SIEMPRE el nombre exacto del catálogo. La cantidad es en bultos.

CONFIRMACIÓN DE CAMBIOS (CRÍTICO):
- Cuando el cliente pide un cambio (agregar/quitar/cambiar cantidad), primero PEDÍ confirmación: "¿Confirmás que agrego X al pedido N° Y?" y devolvé accion_gestion "nada" (todavía no ejecutás).
- Cuando en el turno SIGUIENTE el cliente confirma ("sí", "dale", "confirmo", "correcto"), AHÍ SÍ devolvé accion_gestion "modificar" con el order_number y los cambios EXACTOS que venías de proponer (mirá tu mensaje anterior en el historial para saber qué producto y cantidad era). NO respondas "listo" con accion "nada": si confirmó, el JSON DEBE llevar "modificar" con los cambios, o el pedido NO se actualiza de verdad.
- Si el cliente confirma pero no queda claro qué cambio era, preguntá de nuevo qué quiere agregar en vez de inventar."""



    return f"""{_ctx_tenant(cfg)}

El contacto pidió hablar con una persona. Confirmá que ya estás avisando a alguien del comercio para que lo atienda, de forma cálida y breve.

RESPUESTA JSON OBLIGATORIA (el usuario NO la ve):
---JSON---
{{"escalar": true}}
---FIN---"""


# ─────────────────────────────────────────────
# PEDIDOS (Capa 3)
# ─────────────────────────────────────────────

# Contacto genérico para pruebas (hasta tener la tabla contacts real)
CONTACTO_PRUEBA = "4799b595-5388-4002-8ba8-b9a82624a802"


async def buscar_producto_para_pedido(tenant_id: str, nombres: list) -> list:
    """Busca productos en el catálogo del CPM (fuente de verdad) por nombre, con match robusto.
       Devuelve ids, ambos precios, stocks, promo y fraccionamiento."""
    if not nombres:
        return []
    catalogo = await cpm_get_catalogo(tenant_id)
    out = []
    for nombre in nombres:
        obj = _norm_nombre(nombre)
        # match exacto normalizado, después por contención
        prod = next((p for p in catalogo if _norm_nombre(p.get("name", "")) == obj), None)
        if not prod:
            prod = next((p for p in catalogo
                         if obj and (obj in _norm_nombre(p.get("name", "")) or _norm_nombre(p.get("name", "")) in obj)), None)
        if not prod:
            continue
        promo = prod.get("promo")
        promo_ok = isinstance(promo, dict) and promo.get("activa") and (promo.get("disponibles_en_promo") or 0) > 0
        fr = prod.get("venta_fraccionada") or {}
        out.append({
            "product_id": prod.get("product_id"),
            "product_name": prod.get("name", ""),
            "variant_id": prod.get("variant_id"),
            # compat: "precio" y "disponible" siguen siendo los de bulto (lo que usaba el código previo)
            "precio": float(prod.get("precio_bulto") or 0),
            "disponible": int(prod.get("stock_disponible_bultos") or 0),
            # nuevos campos
            "precio_bulto": float(prod.get("precio_bulto") or 0),
            "precio_unidad": float(prod.get("precio_unidad") or 0) if prod.get("precio_unidad") else None,
            "stock_unidades": prod.get("stock_disponible_unidades"),
            "permite_unidad": bool(fr.get("permite_unidad")),
            "unidades_por_bulto": fr.get("unidades_por_bulto"),
            "promo_activa": bool(promo_ok),
            "precio_promo": float(promo.get("precio_promo") or 0) if promo_ok else None,
            "descuento_pct": promo.get("descuento_pct") if promo_ok else None,
            "disponibles_en_promo": promo.get("disponibles_en_promo") if promo_ok else 0,
        })
    return out


def formato_tabla_pedido(items: list) -> str:
    """Resumen del pedido en tabla monospace numerada con total."""
    if not items:
        return "(pedido vacío)"
    lineas = ["```", "N°  Producto                          Cant   Precio", "──────────────────────────────────────────────────"]
    total = 0
    for i, it in enumerate(items, 1):
        nombre = it["product_name"][:30].ljust(30)
        cant = str(it["cantidad"]).center(5)
        subtotal = it["precio"] * it["cantidad"]
        total += subtotal
        lineas.append(f"{str(i).ljust(3)} {nombre} {cant} ${subtotal:,.0f}")
    lineas.append("──────────────────────────────────────────────────")
    lineas.append(f"TOTAL{' ' * 38}${total:,.0f}")
    lineas.append("```")
    return "\n".join(lineas)


def total_pedido(items: list) -> float:
    return sum(it["precio"] * it["cantidad"] for it in items)


# ─────────────────────────────────────────────
# IMAGEN DEL RESUMEN DE PEDIDO
# ─────────────────────────────────────────────

_LOGO_CACHE = {}  # cache del logo descargado por URL

def _cargar_fuente(size: int, bold: bool = False):
    from PIL import ImageFont
    base = "/usr/share/fonts/truetype/liberation/"
    nombre = "LiberationSans-Bold.ttf" if bold else "LiberationSans-Regular.ttf"
    try:
        return ImageFont.truetype(base + nombre, size)
    except Exception:
        return ImageFont.load_default()


async def _get_logo(logo_url: str):
    """Descarga el logo (con cache en memoria). Devuelve imagen PIL o None."""
    if not logo_url:
        return None
    if logo_url in _LOGO_CACHE:
        return _LOGO_CACHE[logo_url]
    try:
        from PIL import Image
        import io
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(logo_url)
            if r.status_code != 200:
                return None
        logo = Image.open(io.BytesIO(r.content)).convert("RGBA")
        _LOGO_CACHE[logo_url] = logo
        return logo
    except Exception as e:
        print(f"[_get_logo] error: {e}")
        return None


async def generar_imagen_pedido(tenant_id: str, cfg: dict, items: list, delivery: str = "") -> str:
    """Genera la imagen del resumen y la sube a Supabase Storage. Devuelve la URL pública o ''."""
    try:
        from PIL import Image, ImageDraw
        import io, time

        VIOLETA = (123, 77, 224)
        VIOLETA_OSCURO = (74, 61, 158)
        BLANCO = (255, 255, 255)
        GRIS_CLARO = (245, 245, 247)
        GRIS_TEXTO = (90, 90, 100)
        NEGRO_LOGO = (10, 10, 15)
        NEGRO = (30, 30, 35)

        nombre_negocio = cfg.get("rubro") or cfg.get("bot_nombre") or "Pedido"
        logo_url = cfg.get("logo_url", "")

        W = 800
        logo_h = 150 if logo_url else 0
        header_h = 70
        tabla_head = 68
        row_h = 58
        footer_h = 140
        H = logo_h + header_h + tabla_head + row_h * len(items) + footer_h

        img = Image.new("RGB", (W, H), BLANCO)
        d = ImageDraw.Draw(img)

        # Logo
        if logo_url:
            d.rectangle([0, 0, W, logo_h], fill=NEGRO_LOGO)
            logo = await _get_logo(logo_url)
            if logo:
                lw, lh = logo.size
                nh = 120
                nw = int(lw * nh / lh)
                logo = logo.resize((nw, nh))
                img.paste(logo, ((W - nw) // 2, (logo_h - nh) // 2), logo)

        # Banda violeta con nombre
        y0 = logo_h
        d.rectangle([0, y0, W, y0 + header_h], fill=VIOLETA)
        f_neg = _cargar_fuente(26, True)
        # achicar si el nombre es largo
        s = 26
        while d.textlength(nombre_negocio, font=_cargar_fuente(s, True)) > W - 80 and s > 14:
            s -= 1
        d.text((40, y0 + (header_h - s) // 2 - 2), nombre_negocio, font=_cargar_fuente(s, True), fill=BLANCO)

        # Título
        y = y0 + header_h + 20
        d.text((40, y), "RESUMEN DE PEDIDO", font=_cargar_fuente(22, True), fill=VIOLETA_OSCURO)

        # Cabecera tabla
        y += 42
        d.text((40, y), "Producto", font=_cargar_fuente(17, True), fill=GRIS_TEXTO)
        d.text((W - 215, y), "Cant", font=_cargar_fuente(17, True), fill=GRIS_TEXTO)
        d.text((W - 130, y), "Precio", font=_cargar_fuente(17, True), fill=GRIS_TEXTO)
        y += 26
        d.line([40, y, W - 40, y], fill=VIOLETA, width=2)
        y += 8

        # Filas
        total = 0
        for i, it in enumerate(items):
            if i % 2 == 0:
                d.rectangle([40, y, W - 40, y + row_h - 8], fill=GRIS_CLARO)
            nombre = it["product_name"]
            # achicar fuente hasta que entre
            fs = 16
            while d.textlength(nombre, font=_cargar_fuente(fs)) > W - 260 and fs > 11:
                fs -= 1
            d.text((50, y + 16), nombre, font=_cargar_fuente(fs), fill=NEGRO)
            d.text((W - 205, y + 16), str(it["cantidad"]), font=_cargar_fuente(16), fill=NEGRO)
            subtotal = it["precio"] * it["cantidad"]
            total += subtotal
            precio = f"${subtotal:,.0f}".replace(",", ".")
            d.text((W - 130, y + 16), precio, font=_cargar_fuente(16), fill=NEGRO)
            y += row_h

        # Total
        y += 6
        d.line([40, y, W - 40, y], fill=VIOLETA, width=2)
        y += 18
        d.rectangle([W - 330, y, W - 40, y + 52], fill=VIOLETA)
        d.text((W - 315, y + 13), "TOTAL", font=_cargar_fuente(22, True), fill=BLANCO)
        tot = f"${total:,.0f}".replace(",", ".")
        d.text((W - 40 - d.textlength(tot, font=_cargar_fuente(22, True)) - 15, y + 13),
               tot, font=_cargar_fuente(22, True), fill=BLANCO)

        # Footer
        y += 78
        if delivery:
            d.text((40, y), f"Entrega estimada: {delivery}", font=_cargar_fuente(15), fill=GRIS_TEXTO)
        d.text((40, y + 24), "Pendiente de confirmación", font=_cargar_fuente(15, True), fill=VIOLETA)

        # Exportar a bytes
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        png_bytes = buf.getvalue()

        # Subir a Supabase Storage (bucket 'pedidos')
        nombre_archivo = f"{tenant_id}/{int(time.time()*1000)}.png"
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            up = await client.post(
                f"{SUPABASE_URL}/storage/v1/object/pedidos/{nombre_archivo}",
                headers={
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                    "apikey": SUPABASE_KEY,
                    "Content-Type": "image/png",
                    "x-upsert": "true",
                },
                content=png_bytes,
            )
        if up.status_code not in (200, 201):
            print(f"[generar_imagen_pedido] fallo upload: {up.status_code} {up.text[:200]}")
            return ""
        url_publica = f"{SUPABASE_URL}/storage/v1/object/public/pedidos/{nombre_archivo}"
        return url_publica
    except Exception as e:
        print(f"[generar_imagen_pedido] excepción: {e}")
        return ""


# ─────────────────────────────────────────────
# OPERACIONES DE PEDIDO VÍA ENDPOINTS DEL CPM
# ─────────────────────────────────────────────

def _items_cpm_a_imagen(items_cpm: list) -> list:
    """Convierte items del CPM (quantity/unit_price) al formato que espera generar_imagen_pedido (cantidad/precio)."""
    out = []
    for it in items_cpm:
        cant = int(it.get("quantity", it.get("cantidad", 0)) or 0)
        if cant <= 0:
            continue  # ítems borrados (quantity 0) no van en la imagen
        nombre = it.get("product_name", it.get("producto", "Producto"))
        if (it.get("sale_unit") or "bulto") == "unidad" and "(unidad" not in nombre.lower():
            nombre += " (unidad)"
        if it.get("is_promo"):
            nombre += " 🔥PROMO"
        out.append({
            "product_name": nombre,
            "cantidad": cant,
            "precio": float(it.get("unit_price", it.get("precio", 0)) or 0),
        })
    return out


def _headers_cpm():
    return {"Authorization": f"Bearer {CPM_API_KEY}", "Content-Type": "application/json"}


def _norm_nombre(s: str) -> str:
    """Normaliza un nombre de producto para comparar: sin acentos, minúsculas, espacios colapsados."""
    import unicodedata
    s = (s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return " ".join(s.split())


def _buscar_item_pedido(items_actuales: list, nombre: str):
    """Encuentra un ítem del pedido por nombre, tolerante a acentos/mayúsculas/espacios.
       Primero exacto normalizado; si no, por contención (uno dentro del otro)."""
    objetivo = _norm_nombre(nombre)
    # 1) match exacto normalizado
    for it in items_actuales:
        if _norm_nombre(it.get("product_name", "")) == objetivo:
            return it
    # 2) match por contención (el nombre del cliente está contenido en el del pedido o viceversa)
    for it in items_actuales:
        n = _norm_nombre(it.get("product_name", ""))
        if objetivo and (objetivo in n or n in objetivo):
            return it
    return None





ESTADO_NATURAL = {
    "pendiente": "pendiente (todavía no lo empezó a preparar el local)",
    "en_preparacion": "en preparación (el local ya lo está armando)",
    "para_enviar": "listo para salir a reparto",
    "en_entrega": "en camino",
    "entregado": "entregado",
    "cancelado": "cancelado",
}


def _estado_natural(status: str) -> str:
    return ESTADO_NATURAL.get((status or "").strip().lower(), (status or "desconocido").replace("_", " "))


def formato_pedidos_gestion(pedidos: list) -> str:
    """Arma el texto de pedidos del cliente para el prompt de gestión."""
    if not pedidos:
        return "(el cliente no tiene pedidos registrados)"
    # ordenar por fecha desc si viene el campo; si no, dejar el orden recibido
    def _fecha(p):
        return p.get("created_at") or p.get("createdAt") or ""
    try:
        pedidos = sorted(pedidos, key=_fecha, reverse=True)
    except Exception:
        pass
    lineas = []
    for p in pedidos[:8]:
        num = p.get("order_number") or "?"
        status = p.get("status", "?")
        items = p.get("items") or []
        partes = []
        total_calc = 0
        for it in items:
            cant = int(it.get("quantity", it.get("cantidad", 0)) or 0)
            nombre = it.get("product_name", it.get("producto", "producto"))
            precio = float(it.get("unit_price", it.get("precio", 0)) or 0)
            subtotal = precio * cant
            total_calc += subtotal
            etiquetas = []
            if (it.get("sale_unit") or "bulto") == "unidad":
                etiquetas.append("unidad suelta")
            if it.get("is_promo"):
                etiquetas.append("EN PROMO")
            etq = f" [{', '.join(etiquetas)}]" if etiquetas else ""
            partes.append(f"{cant}x {nombre}{etq} (${precio:,.0f} c/u = ${subtotal:,.0f})")
        items_txt = "; ".join(partes) or "(sin detalle de ítems)"
        # total: usar el del CPM si viene, si no el calculado
        total = p.get("total")
        total_val = float(total) if total is not None else total_calc
        editable = "SÍ (está pendiente)" if str(status).strip().lower() == "pendiente" else "NO"
        cancelable = "SÍ" if str(status).strip().lower() in ("pendiente", "confirmado", "en_preparacion", "para_enviar") else "NO"
        lineas.append(
            f"- Pedido N° {num} | estado: {_estado_natural(status)} | "
            f"modificable: {editable} | cancelable: {cancelable} | "
            f"TOTAL: ${total_val:,.0f} | productos: {items_txt}"
        )
    return "\n".join(lineas)


async def cpm_crear_pedido(tenant_id: str, manychat_contact_id: str, items: list) -> dict:
    """POST /orders — crea el pedido en estado pendiente. Devuelve {ok, order_id, order_number}.
       Si el CPM rechaza (400 con mensaje: sin cupo promo, sin stock, etc.), devuelve {ok:False, error}."""
    payload_items = []
    for it in items:
        item = {
            "variant_id": it["variant_id"],
            "product_id": it["product_id"],
            "product_name": it["product_name"],
            "quantity": it["cantidad"],
            "unit_price": it["precio"],
        }
        if it.get("sale_unit") and it["sale_unit"] != "bulto":
            item["sale_unit"] = it["sale_unit"]
        if it.get("is_promo"):
            item["sale_unit"] = it.get("sale_unit", "bulto")
            item["is_promo"] = True
        payload_items.append(item)
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.post(
                f"{CPM_API_URL}/orders",
                headers=_headers_cpm(),
                json={"tenant_id": tenant_id, "manychat_contact_id": manychat_contact_id, "items": payload_items},
            )
        if r.status_code not in (200, 201):
            print(f"[cpm_crear_pedido] status={r.status_code} resp={r.text[:200]}")
            err = ""
            try:
                err = (r.json() or {}).get("error", "")
            except Exception:
                pass
            return {"ok": False, "error": err}
        data = r.json()
        return {"ok": True, "order_id": data.get("order_id"), "order_number": data.get("order_number")}
    except Exception as e:
        print(f"[cpm_crear_pedido] excepción: {e}")
        return {"ok": False, "error": ""}


async def cpm_consultar_pedidos_cliente(tenant_id: str, manychat_contact_id: str) -> list:
    """GET /orders?manychat_contact_id — lista de pedidos del cliente con estado e ítems."""
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(
                f"{CPM_API_URL}/orders",
                headers=_headers_cpm(),
                params={"tenant_id": tenant_id, "manychat_contact_id": manychat_contact_id},
            )
        if r.status_code != 200:
            print(f"[cpm_consultar_pedidos_cliente] status={r.status_code} resp={r.text[:200]}")
            return []
        data = r.json()
        if isinstance(data, dict):
            return data.get("orders", [])
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[cpm_consultar_pedidos_cliente] excepción: {e}")
        return []


async def cpm_consultar_pedido(tenant_id: str, order_id: str) -> dict:
    """GET /orders/{id} — pedido completo con ítems (cada uno con su id)."""
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(
                f"{CPM_API_URL}/orders/{order_id}",
                headers=_headers_cpm(),
                params={"tenant_id": tenant_id},
            )
        if r.status_code != 200:
            print(f"[cpm_consultar_pedido] status={r.status_code} resp={r.text[:200]}")
            return {}
        data = r.json()
        if not isinstance(data, dict):
            return {}
        # Formato robusto: puede venir directo, envuelto en "order", o en "orders":[...]
        ped = None
        if "items" in data or "order_items" in data:
            ped = data
        elif isinstance(data.get("order"), dict):
            ped = data["order"]
        elif isinstance(data.get("orders"), list) and data["orders"]:
            ped = data["orders"][0]
        else:
            ped = data
        # Normalizar: el GET único devuelve los ítems en "order_items"; el de lista en "items".
        # Dejamos siempre "items" disponible para el resto del código.
        if isinstance(ped, dict) and "items" not in ped and "order_items" in ped:
            ped["items"] = ped.get("order_items") or []
        return ped
    except Exception as e:
        print(f"[cpm_consultar_pedido] excepción: {e}")
        return {}


async def cpm_cambiar_estado(tenant_id: str, order_id: str, status: str) -> bool:
    """PATCH /orders/{id}/status — cambia el estado del pedido."""
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.patch(
                f"{CPM_API_URL}/orders/{order_id}/status",
                headers=_headers_cpm(),
                json={"tenant_id": tenant_id, "status": status},
            )
        if r.status_code not in (200, 204):
            print(f"[cpm_cambiar_estado] status={r.status_code} resp={r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"[cpm_cambiar_estado] excepción: {e}")
        return False


async def cpm_editar_items(tenant_id: str, order_id: str, items: list) -> dict:
    """PATCH /orders/{id}/items — incremental. items acepta:
       {id, quantity} para cambiar cantidad, {id, quantity:0} para quitar,
       {variant_id, product_id, product_name, quantity, unit_price, sale_unit?, is_promo?} para agregar.
       Devuelve {ok: bool, error: str} — error trae el mensaje de negocio del CPM (400)."""
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.patch(
                f"{CPM_API_URL}/orders/{order_id}/items",
                headers=_headers_cpm(),
                json={"tenant_id": tenant_id, "items": items},
            )
        if r.status_code not in (200, 204):
            print(f"[cpm_editar_items] status={r.status_code} resp={r.text[:200]}")
            err = ""
            try:
                err = (r.json() or {}).get("error", "")
            except Exception:
                pass
            return {"ok": False, "error": err}
        return {"ok": True, "error": ""}
    except Exception as e:
        print(f"[cpm_editar_items] excepción: {e}")
        return {"ok": False, "error": ""}


async def cpm_cancelar_pedido(tenant_id: str, order_id: str) -> bool:
    """POST /orders/{id}/cancel — cancela el pedido."""
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.post(
                f"{CPM_API_URL}/orders/{order_id}/cancel",
                headers=_headers_cpm(),
                json={"tenant_id": tenant_id},
            )
        if r.status_code not in (200, 204):
            print(f"[cpm_cancelar_pedido] status={r.status_code} resp={r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"[cpm_cancelar_pedido] excepción: {e}")
        return False


async def registrar_pedido(tenant_id: str, contact_uuid: str, items: list, direccion: str = "") -> dict:
    """Crea orders + order_items y reserva stock. Devuelve {ok, order_id} o {ok: False, error}."""
    from datetime import timedelta
    total = total_pedido(items)
    delivery = (datetime.utcnow() + timedelta(days=1)).date().isoformat()
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            # 1) Crear la orden
            r = await client.post(
                f"{SUPABASE_URL}/rest/v1/orders",
                headers=_headers({"Content-Type": "application/json", "Prefer": "return=representation"}),
                json={
                    "tenant_id": tenant_id,
                    "contact_id": contact_uuid,
                    "status": "pendiente",
                    "total": total,
                    "source": "whatsapp",
                    "delivery_date": delivery,
                    "delivery_address": direccion or None,
                }
            )
            orden = r.json()
            if not (isinstance(orden, list) and len(orden) > 0 and orden[0].get("id")):
                print(f"[registrar_pedido] fallo al crear orden: status={r.status_code} resp={orden}")
                return {"ok": False, "error": "No se pudo crear la orden"}
            order_id = orden[0]["id"]

            # 2) Crear los items
            items_payload = [{
                "tenant_id": tenant_id,
                "order_id": order_id,
                "product_id": it["product_id"],
                "variant_id": it["variant_id"],
                "quantity": it["cantidad"],
                "unit_price": it["precio"],
                "line_total": it["precio"] * it["cantidad"],
            } for it in items]
            r2 = await client.post(
                f"{SUPABASE_URL}/rest/v1/order_items",
                headers=_headers({"Content-Type": "application/json", "Prefer": "return=minimal"}),
                json=items_payload
            )
            if r2.status_code >= 300:
                print(f"[registrar_pedido] fallo al crear items: status={r2.status_code} resp={r2.text[:200]}")

            # 3) Reservar stock (sumar a reserved de cada variante)
            for it in items:
                rv = await client.get(
                    f"{SUPABASE_URL}/rest/v1/product_variants",
                    headers=_headers(),
                    params={"id": f"eq.{it['variant_id']}", "select": "reserved"}
                )
                actual = rv.json()
                reserved_actual = (actual[0].get("reserved") or 0) if isinstance(actual, list) and actual else 0
                await client.patch(
                    f"{SUPABASE_URL}/rest/v1/product_variants",
                    headers=_headers({"Content-Type": "application/json", "Prefer": "return=minimal"}),
                    params={"id": f"eq.{it['variant_id']}"},
                    json={"reserved": reserved_actual + it["cantidad"]}
                )
        return {"ok": True, "order_id": order_id, "total": total, "delivery": delivery}
    except Exception as e:
        print(f"[registrar_pedido] excepción: {e}")
        return {"ok": False, "error": str(e)}


# ─────────────────────────────────────────────
# AUDIO (Capa 4 — Whisper)
# ─────────────────────────────────────────────

def tipo_de_url(texto: str) -> str:
    """Devuelve 'audio', 'imagen' o 'texto' según el contenido del mensaje."""
    if not texto:
        return "texto"
    t = texto.strip().lower()
    if not t.startswith("http") or " " in t:
        return "texto"
    audio_ext = (".ogg", ".oga", ".opus", ".mp3", ".m4a", ".wav", ".webm", ".mp4", ".mpeg", ".mpga")
    img_ext = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")
    # cortar querystring si la hay
    base = t.split("?")[0]
    if base.endswith(audio_ext):
        return "audio"
    if base.endswith(img_ext):
        return "imagen"
    # si es URL pero no reconocemos extensión, asumimos imagen (las de ManyChat a veces no traen ext clara)
    return "imagen"


async def transcribir_audio(audio_url: str) -> str:
    """Descarga el audio y lo transcribe con Whisper (OpenAI). Devuelve texto o None."""
    if not OPENAI_KEY:
        print("[transcribir_audio] falta OPENAI_KEY")
        return None
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            ar = await client.get(audio_url)
            if ar.status_code != 200:
                print(f"[transcribir_audio] descarga falló status={ar.status_code}")
                return None
            audio_bytes = ar.content
            r = await client.post(
                "https://api.openai.com/v1/audio/transcriptions",
                headers={"Authorization": f"Bearer {OPENAI_KEY}"},
                files={"file": ("audio.ogg", audio_bytes, "audio/ogg")},
                data={"model": "whisper-1", "language": "es"}
            )
        data = r.json()
        if "text" not in data:
            print(f"[transcribir_audio] sin text | status={r.status_code} resp={data}")
            return None
        return data["text"].strip()
    except Exception as e:
        print(f"[transcribir_audio] excepción: {e}")
        return None


async def leer_imagen(imagen_url: str, lista_catalogo: str) -> dict:
    """Claude lee la imagen (factura/nota/lista) y extrae productos+cantidades cruzando con el catálogo.
    Devuelve {tipo: 'pedido'|'descripcion', items: [...], texto: '...'}."""
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            ir = await client.get(imagen_url)
            if ir.status_code != 200:
                print(f"[leer_imagen] descarga falló status={ir.status_code}")
                return {"tipo": "error", "items": [], "texto": ""}
            import base64
            img_b64 = base64.standard_b64encode(ir.content).decode("utf-8")
            media_type = ir.headers.get("content-type", "image/jpeg")
            if "jpeg" in media_type or "jpg" in media_type:
                media_type = "image/jpeg"
            elif "png" in media_type:
                media_type = "image/png"
            elif "webp" in media_type:
                media_type = "image/webp"
            else:
                media_type = "image/jpeg"

            prompt_vision = f"""Sos un asistente que lee imágenes para tomar pedidos de una distribuidora. El cliente puede mandar:
- Una factura o remito anterior.
- Una nota o lista escrita a mano o impresa.
- Una foto de un producto donde se vea su nombre, etiqueta o envase.

Tu tarea: identificá qué PRODUCTOS y CANTIDADES aparecen en la imagen, y cruzalos con este catálogo usando los nombres EXACTOS del catálogo. Interpretá con flexibilidad: si en la imagen ves un nombre, fragancia, formato (ml) o tipo de producto que se corresponde con algo del catálogo, hacé el match aunque no esté escrito idéntico (ej. "piso marina 27" → "Bulto Limpiador de Pisos Smart Marina 27ml"). Si no hay cantidad indicada, asumí 1.

CATÁLOGO:
{lista_catalogo}

Respondé SOLO con este JSON:
{{"tipo": "pedido" o "descripcion", "items": [{{"producto": "nombre exacto del catálogo", "cantidad": N}}], "texto": "qué ves en la imagen, breve"}}

- Si identificás uno o más productos que matchean el catálogo → tipo "pedido", completá items con nombres exactos y cantidades.
- Si ves productos que NO están en el catálogo, no los inventes; mencionalos en "texto" pero no los pongas en items.
- Si la imagen no tiene productos reconocibles, está borrosa o no se entiende → tipo "descripcion", items vacío, y en "texto" describí brevemente qué ves.
- Nunca inventes productos que no estén en el catálogo."""

            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
                json={
                    "model": MODELO,
                    "max_tokens": 800,
                    "messages": [{
                        "role": "user",
                        "content": [
                            {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": img_b64}},
                            {"type": "text", "text": prompt_vision}
                        ]
                    }]
                }
            )
        data = r.json()
        if "content" not in data:
            print(f"[leer_imagen] sin content | status={r.status_code} resp={data}")
            return {"tipo": "error", "items": [], "texto": ""}
        raw = data["content"][0]["text"]
        jd = _extraer_json(raw)
        return {
            "tipo": jd.get("tipo", "descripcion"),
            "items": jd.get("items", []),
            "texto": jd.get("texto", ""),
        }
    except Exception as e:
        print(f"[leer_imagen] excepción: {e}")
        return {"tipo": "error", "items": [], "texto": ""}


RUTAS_VALIDAS = ("ASESOR", "PEDIDO", "GESTION", "CONTINUAR", "CHARLA", "AGENTE_HUMANO")
AGENTES_CONTENIDO = ("asesor", "pedido", "gestion", "agente_humano")


async def _interpretar_desambiguacion(cfg: dict, mensaje: str) -> str:
    """Decide si el cliente quiere 'sumar' al pedido de hoy o armar uno 'nuevo'.
       Devuelve 'sumar' o 'nuevo'. Ante duda, default 'sumar' (decisión de negocio)."""
    m = (mensaje or "").lower()
    # Atajos por palabras claras, sin gastar una llamada al modelo
    if any(k in m for k in ["nuevo", "otro", "aparte", "separado", "distinto", "por separado"]):
        return "nuevo"
    if any(k in m for k in ["sum", "agreg", "añad", "anad", "a ese", "al mismo", "al pedido", "junto", "mismo pedido", "ese pedido"]):
        return "sumar"
    # Caso ambiguo → una consulta corta al modelo
    prompt = (f"{_ctx_tenant(cfg)}\n\nEl cliente ya tiene un pedido de hoy y le preguntaste si quiere "
              f"SUMARLE productos a ese pedido o armar uno NUEVO aparte. Su respuesta fue: \"{mensaje}\".\n"
              f"Devolvé SOLO una palabra: 'sumar' si quiere agregar al pedido existente, 'nuevo' si quiere uno aparte. "
              f"Si no queda claro, respondé 'sumar'.")
    raw = await llamar_claude(prompt, [{"role": "user", "content": mensaje}], max_tokens=10)
    resp = (raw or "").strip().lower()
    return "nuevo" if "nuevo" in resp else "sumar"


async def clasificar_ruta(cfg: dict, historial: list, mensaje: str, tarea: str, hay_carrito: bool = False) -> str:
    hist = historial[-6:] if len(historial) > 6 else historial
    contexto = ""
    if hay_carrito:
        contexto = "[CONTEXTO: hay un PEDIDO EN CURSO con productos en el carrito. Cualquier pregunta sobre precio, total, resumen o cambios al pedido es PEDIDO, no ASESOR.]"
    elif tarea == "gestion":
        contexto = "[CONTEXTO: hay una GESTIÓN DE PEDIDO en curso (el cliente está consultando, modificando o cancelando un pedido ya confirmado, y puede haber una confirmación pendiente). Si el contacto sigue el hilo — confirma ('sí', 'dale'), da más datos, o sigue hablando del mismo pedido — es CONTINUAR.]"
    elif tarea in AGENTES_CONTENIDO:
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
    imagen_url = ""  # URL de imagen del resumen de pedido, si se genera

    conv = await get_conversacion(tenant_id, contact_id)
    historial = conv["historial"]
    if len(historial) > 40:
        historial = historial[-40:]
    tarea = conv["tarea"]

    # ¿hay un pedido en curso? (carrito con productos)
    carrito_previo = conv["pedido"] if isinstance(conv["pedido"], list) else []
    hay_carrito = len(carrito_previo) > 0

    ruta = await clasificar_ruta(cfg, historial, mensaje, tarea, hay_carrito)

    if ruta == "CONTINUAR":
        agente = tarea if tarea in AGENTES_CONTENIDO else "asesor"
    elif ruta == "ASESOR":
        agente = "asesor"
    elif ruta == "PEDIDO":
        agente = "pedido"
    elif ruta == "GESTION":
        agente = "gestion"
    elif ruta == "AGENTE_HUMANO":
        agente = "agente_humano"
    else:
        agente = "charla"

    # Si hay carrito en curso y cae en asesor/charla, forzar pedido (red de seguridad)
    if hay_carrito and agente in ("asesor", "charla"):
        agente = "pedido"

    # Red de seguridad de GESTIÓN: si venías gestionando un pedido confirmado y el router
    # te manda a pedido/asesor/charla SIN señal clara de "pedido nuevo" o cierre, mantené gestión.
    # Evita que "agregá un combo" o una cortesía ("perfecto") saquen del hilo de gestión.
    if tarea == "gestion" and agente in ("pedido", "asesor", "charla"):
        m_low = (mensaje or "").lower()
        sale_de_gestion = any(k in m_low for k in [
            "pedido nuevo", "otro pedido", "nuevo pedido", "empezar de cero",
            "arrancar otro", "aparte", "por separado", "distinto pedido",
            "chau", "nada mas", "nada más", "eso es todo", "listo gracias"])
        # cortesías puras ("perfecto", "gracias", "ok") NO sacan de gestión: se quedan
        if not sale_de_gestion:
            print(f"[DIAG-GESTION] router dijo '{agente}' pero mantengo GESTION (venía gestionando)")
            agente = "gestion"

    # ── DESAMBIGUACIÓN "pedido del día" (sin consultar CPM, solo con la marca local) ──
    # Si el cliente arranca un pedido nuevo (no hay carrito) y YA confirmó un pedido HOY,
    # preguntamos si sumar a ese o armar uno nuevo, en vez de asumir.
    hoy_iso = datetime.utcnow().date().isoformat()
    ultimo_ped_fecha = conv.get("ultimo_pedido_fecha", "")
    ultimo_ped_num = conv.get("ultimo_pedido_num", "")
    hubo_pedido_hoy = (ultimo_ped_fecha == hoy_iso) and bool(ultimo_ped_num)

    if agente == "pedido" and not hay_carrito and hubo_pedido_hoy and tarea != "desambiguar_pedido":
        # Interceptar: preguntar antes de armar carrito
        historial.append({"role": "user", "content": mensaje})
        texto = (f"Veo que hoy ya hiciste el pedido N° {ultimo_ped_num}. "
                 f"¿Querés que le sume estos productos a ese pedido, o preferís armar uno nuevo aparte?")
        historial.append({"role": "assistant", "content": texto})
        await upsert_conversacion(tenant_id, contact_id, {
            "historial": historial,
            "agente_activo": "pedido",
            "tarea_pendiente": "desambiguar_pedido",
            "pedido_en_curso": carrito_previo,
        })
        await guardar_log(tenant_id, contact_id, "pedido", mensaje, texto)
        return "pedido", texto, {}, ""

    # Si venís de la pregunta de desambiguación, interpretar la respuesta
    if tarea == "desambiguar_pedido":
        eleccion = await _interpretar_desambiguacion(cfg, mensaje)
        print(f"[DIAG-DESAMB] eleccion={eleccion} | ultimo_num={ultimo_ped_num}")
        if eleccion == "sumar":
            agente = "gestion"  # gestión sumará al pedido existente
        else:
            agente = "pedido"   # armar carrito nuevo
        # limpiamos la tarea de desambiguación (ya se resolvió)
        tarea = ""

    # Carrito actual (lo necesita el agente pedido)
    carrito = carrito_previo
    pedido_registrado = None
    marca_pedido = None  # (fecha_iso, order_number) si se confirma un pedido en este turno
    gestion_completada = False  # True cuando gestión ejecutó una acción y cierra el hilo

    historial.append({"role": "user", "content": mensaje})

    if agente == "charla":
        raw = await llamar_claude(prompt_charla(cfg), historial, max_tokens=300)
    elif agente == "asesor":
        if es_consulta_amplia(mensaje):
            # Consulta amplia: mostrar categorías + destacados
            cats_txt = await get_categorias_con_destacados(tenant_id)
            raw = await llamar_claude(
                prompt_asesor_catalogo(cfg, cats_txt),
                historial, max_tokens=600
            )
        else:
            # Producto puntual: flujo de dos pasos
            lista = await get_lista_liviana(tenant_id)
            raw1 = await llamar_claude(
                prompt_asesor_paso1(cfg, formato_lista_liviana(lista)),
                historial, max_tokens=200
            )
            _, jd1 = parsear_respuesta(raw1)
            nombres = jd1.get("productos") or []
            detalle = await get_detalle_productos(tenant_id, nombres) if nombres else []
            raw = await llamar_claude(
                prompt_asesor_paso2(cfg, formato_detalle(detalle)),
                historial, max_tokens=700
            )
    elif agente == "pedido":
        lista = await get_lista_liviana(tenant_id)
        carrito_txt = formato_tabla_pedido(carrito) if carrito else "(vacío)"
        # Snapshot del carrito ANTES de procesar (para detectar cambios reales)
        import copy
        carrito_antes = copy.deepcopy(carrito)
        raw = await llamar_claude(
            prompt_pedido(cfg, formato_lista_liviana(lista), carrito_txt),
            historial, max_tokens=600
        )
        texto_tmp, jd_ped = parsear_respuesta(raw)
        accion = (jd_ped.get("accion") or "nada").lower()
        print(f"[DIAG-PEDIDO] accion='{accion}' | jd_ped={jd_ped} | raw={raw[:200]}")

        if accion in ("agregar", "reemplazar"):
            items_ped = jd_ped.get("items") or []
            nombres = [it.get("producto", "") for it in items_ped]
            cants = {it.get("producto", ""): int(it.get("cantidad", 1) or 1) for it in items_ped}
            unidades = {it.get("producto", ""): (it.get("unidad") or "bulto").lower() for it in items_ped}
            encontrados = await buscar_producto_para_pedido(tenant_id, nombres)

            # ANTI-DUPLICADO (idempotencia): el modelo suele re-emitir el MISMO
            # bloque "agregar" cuando el cliente responde "sí"/"dale"/"confirmo"
            # sin pedir productos nuevos. Si TODOS los productos de este "agregar"
            # ya están en el carrito con cantidad >= a la pedida, es un re-envío,
            # no una intención real de sumar más: lo tratamos como no-op.
            if accion == "agregar" and encontrados:
                def _cant_en_carrito(pid, su):
                    it = next((c for c in carrito if c["product_id"] == pid
                               and c.get("sale_unit", "bulto") == su), None)
                    return it["cantidad"] if it else 0
                es_reenvio = all(
                    _cant_en_carrito(prod["product_id"], unidades.get(prod["product_name"], "bulto")) >= cants.get(prod["product_name"], 1)
                    for prod in encontrados
                )
                if es_reenvio:
                    print(f"[DIAG-PEDIDO] agregar IGNORADO por re-envío (idempotencia): "
                          f"{[(p['product_name'], cants.get(p['product_name'],1)) for p in encontrados]}")
                    texto = texto_tmp
                    accion = "nada"  # neutraliza: no cambia el carrito ni la imagen
                    encontrados = []

            avisos = []
            for prod in encontrados:
                pedido_cant = cants.get(prod["product_name"], 1)
                su = unidades.get(prod["product_name"], "bulto")
                # Venta por unidad: validar que el producto lo permita y el stock alcance
                if su == "unidad":
                    if not prod.get("permite_unidad") or not prod.get("precio_unidad"):
                        avisos.append(f"⚠️ {prod['product_name']}: solo se vende por bulto, lo dejé como bulto.")
                        su = "bulto"
                    elif prod.get("stock_unidades") is not None and pedido_cant > prod["stock_unidades"]:
                        avisos.append(f"⚠️ {prod['product_name']}: solo hay {prod['stock_unidades']} unidades sueltas, ajusté.")
                        pedido_cant = prod["stock_unidades"]
                if su == "bulto":
                    if prod["disponible"] <= 0:
                        avisos.append(f"⚠️ {prod['product_name']}: sin stock, no lo pude agregar.")
                        continue
                    if pedido_cant > prod["disponible"]:
                        avisos.append(f"⚠️ {prod['product_name']}: solo hay {prod['disponible']} disponibles, ajusté la cantidad.")
                        pedido_cant = prod["disponible"]
                if pedido_cant <= 0:
                    continue
                # Precio según cómo compra: promo (solo bulto) > bulto > unidad
                is_promo = False
                if su == "unidad":
                    precio = prod["precio_unidad"]
                elif prod.get("promo_activa") and (prod.get("disponibles_en_promo") or 0) >= pedido_cant:
                    precio = prod["precio_promo"]
                    is_promo = True
                else:
                    precio = prod["precio_bulto"]
                existente = next((c for c in carrito if c["product_id"] == prod["product_id"]
                                  and c.get("sale_unit", "bulto") == su), None)
                if existente:
                    if accion == "reemplazar":
                        existente["cantidad"] = pedido_cant
                    else:
                        existente["cantidad"] += pedido_cant
                    existente["precio"] = precio
                    existente["is_promo"] = is_promo
                else:
                    nombre_mostrar = prod["product_name"] + (" (unidad)" if su == "unidad" else "")
                    carrito.append({
                        "product_id": prod["product_id"],
                        "product_name": nombre_mostrar,
                        "variant_id": prod["variant_id"],
                        "precio": precio,
                        "cantidad": pedido_cant,
                        "sale_unit": su,
                        "is_promo": is_promo,
                    })
            texto = texto_tmp
            if avisos:
                texto += "\n\n" + "\n".join(avisos)

        elif accion == "confirmar":
            if not carrito:
                texto = "No tengo productos en el pedido todavía. ¿Qué te gustaría llevar?"
            else:
                total = total_pedido(carrito)
                # Crear el pedido en el CPM (estado pendiente), vinculado al contacto de ManyChat
                res = await cpm_crear_pedido(tenant_id, contact_id, carrito)
                if res.get("ok"):
                    num = res.get("order_number", "")
                    texto = (f"{texto_tmp}\n\n✅ ¡Pedido registrado{f' (N° {num})' if num else ''}! "
                             f"Total: ${total:,.0f}. En breve el equipo lo confirma. ¡Gracias!")
                    # Marca local para desambiguar futuros pedidos del día (sin consultar CPM)
                    marca_pedido = (datetime.utcnow().date().isoformat(), str(num))
                    pedido_registrado = res
                    carrito = []
                elif res.get("error"):
                    # Error de NEGOCIO del CPM (sin cupo de promo, sin stock, no fraccionable):
                    # comunicarlo y MANTENER el carrito para que el cliente ajuste y reintente.
                    print(f"[cpm_crear_pedido] rechazo de negocio: {res['error']}")
                    texto = (f"No pude registrar el pedido: {res['error']} "
                             f"¿Querés ajustar la cantidad o cambiar algo y lo intentamos de nuevo?")
                else:
                    # Fallo técnico (sin mensaje): no acumular basura entre sesiones.
                    print(f"[cpm_crear_pedido] FALLÓ al confirmar (técnico) — carrito se vacía para no acumular. total=${total:,.0f}")
                    texto = ("Tomé tu pedido y lo estoy registrando. Si en un rato no te llega la confirmación, "
                             "escribinos y lo revisamos. ¡Gracias!")
                    pedido_registrado = {"ok": False, "cerrado": True}
                    carrito = []
        else:
            texto = texto_tmp

        # IMAGEN: solo si el carrito CAMBIÓ o si el cliente pidió ver el resumen.
        def _resumen_carrito(c):
            return sorted([(x["product_id"], x["cantidad"]) for x in c])
        cambio = _resumen_carrito(carrito_antes) != _resumen_carrito(carrito)
        pidio_resumen = accion == "resumen"
        print(f"[DIAG-IMG] cambio={cambio} | pidio_resumen={pidio_resumen} | items={len(carrito)}")
        if carrito and not pedido_registrado and (cambio or pidio_resumen):
            from datetime import timedelta
            delivery = (datetime.utcnow() + timedelta(days=1)).date().strftime("%d/%m/%Y")
            imagen_url = await generar_imagen_pedido(tenant_id, cfg, carrito, delivery)
            print(f"[DIAG-IMG] imagen_url='{imagen_url}'")
            if not imagen_url:
                # respaldo: si falla la imagen, mostramos la tabla de texto
                texto += "\n\n" + formato_tabla_pedido(carrito)
    elif agente == "gestion":
        # Gestión de pedidos YA confirmados: consultar estado, modificar (agregar/quitar/cambiar, solo pendiente), cancelar.
        pedidos = await cpm_consultar_pedidos_cliente(tenant_id, contact_id)
        lista_g = await get_lista_liviana(tenant_id)
        # Si hay un carrito armándose, puede ser lo que el cliente quiere sumar a un pedido previo
        carrito_ctx = ""
        if carrito:
            prods_carrito = ", ".join(f"{c['cantidad']}x {c['product_name']}" for c in carrito)
            carrito_ctx = (f"\n\nEL CLIENTE TIENE ESTOS PRODUCTOS SIN CONFIRMAR (carrito actual): {prods_carrito}. "
                           f"Si pide 'sumá esto / agregá estos al pedido anterior', SON ESTOS productos los que hay que agregar (operacion 'agregar').")
        print(f"[DIAG-GESTION] pedidos_encontrados={len(pedidos)} | carrito_pendiente={len(carrito)}")
        raw = await llamar_claude(
            prompt_gestion(cfg, formato_pedidos_gestion(pedidos), formato_lista_liviana(lista_g) + carrito_ctx),
            historial, max_tokens=500
        )
        texto, jd_g = parsear_respuesta(raw)
        acc_g = (jd_g.get("accion_gestion") or "nada").lower()
        num_g = str(jd_g.get("order_number") or "").strip()
        print(f"[DIAG-GESTION] accion={acc_g} | order_number={num_g} | jd={jd_g}")

        # Resolver el pedido objetivo: por número si lo dio, si no el más reciente
        def _match_pedido(p):
            pn = str(p.get("order_number") or "").strip()
            return pn == num_g
        objetivo = None
        if pedidos:
            if num_g:
                objetivo = next((p for p in pedidos if _match_pedido(p)), None)
            if not objetivo and not num_g:
                # más reciente
                try:
                    objetivo = sorted(pedidos, key=lambda p: p.get("created_at", ""), reverse=True)[0]
                except Exception:
                    objetivo = pedidos[0]

        if acc_g == "cancelar" and objetivo:
            estado = str(objetivo.get("status", "")).strip().lower()
            oid = objetivo.get("order_id") or objetivo.get("id")
            if estado in ("pendiente", "confirmado", "en_preparacion", "para_enviar"):
                ok = await cpm_cancelar_pedido(tenant_id, oid)
                if ok:
                    texto = f"Listo, cancelé tu pedido N° {objetivo.get('order_number', num_g)}. Cualquier cosa, acá estoy."
                    gestion_completada = True
                else:
                    texto = "No pude cancelarlo desde acá. Te paso con el equipo para que lo resuelvan."
            else:
                texto = (f"Tu pedido N° {objetivo.get('order_number', num_g)} ya está {_estado_natural(estado)}, "
                         f"así que no lo puedo cancelar desde acá. Escribile al equipo para verlo.")

        elif acc_g == "modificar" and objetivo:
            estado = str(objetivo.get("status", "")).strip().lower()
            oid = objetivo.get("order_id") or objetivo.get("id")
            if estado == "pendiente":
                cambios = jd_g.get("cambios") or []
                # GET FRESCO del pedido justo antes de armar el PATCH: los id de ítem
                # no son estables (si el pedido se modificó antes, los viejos quedan muertos).
                # Tomamos los id del pedido correcto y recién actualizado.
                ped_fresco = await cpm_consultar_pedido(tenant_id, oid)
                items_actuales = (ped_fresco.get("items") if isinstance(ped_fresco, dict) else None)
                if not items_actuales:
                    # respaldo: si el GET único no trae items, usar los del listado inicial
                    items_actuales = objetivo.get("items") or []
                print(f"[DIAG-GESTION] items_frescos={len(items_actuales)} (del GET previo al PATCH)")
                payload = []
                # Nombres de productos NUEVOS a agregar (necesitan datos de catálogo)
                nombres_nuevos = [c.get("producto", "") for c in cambios
                                  if (c.get("operacion") or "").lower() == "agregar"]
                catalogo_nuevos = await buscar_producto_para_pedido(tenant_id, nombres_nuevos) if nombres_nuevos else []
                cat_por_nombre = {p["product_name"].lower(): p for p in catalogo_nuevos}

                no_encontrados = []
                for c in cambios:
                    op = (c.get("operacion") or "").lower()
                    nombre = c.get("producto", "")
                    cant = int(c.get("cantidad", 1) or 1)
                    existente = _buscar_item_pedido(items_actuales, nombre)
                    if op == "quitar":
                        if existente and existente.get("id"):
                            payload.append({"id": existente["id"], "quantity": 0})
                        else:
                            no_encontrados.append(nombre)
                    elif op == "cambiar":
                        if existente and existente.get("id"):
                            payload.append({"id": existente["id"], "quantity": cant})
                        else:
                            no_encontrados.append(nombre)
                    elif op == "agregar":
                        # Si ya estaba, "agregar" suma a lo existente → cambiar cantidad total
                        if existente and existente.get("id"):
                            actual = int(existente.get("quantity", 0) or 0)
                            payload.append({"id": existente["id"], "quantity": actual + cant})
                        else:
                            prod = cat_por_nombre.get(nombre.lower()) or next(
                                (p for p in catalogo_nuevos if _norm_nombre(p["product_name"]) == _norm_nombre(nombre)), None)
                            if prod and prod.get("disponible", 0) > 0:
                                # Precio según promo (la promo aplica solo por bulto)
                                is_promo = bool(prod.get("promo_activa") and (prod.get("disponibles_en_promo") or 0) >= cant)
                                precio = prod["precio_promo"] if is_promo else prod["precio_bulto"]
                                item_nuevo = {
                                    "variant_id": prod["variant_id"],
                                    "product_id": prod["product_id"],
                                    "product_name": prod["product_name"],
                                    "quantity": cant,
                                    "unit_price": precio,
                                }
                                if is_promo:
                                    item_nuevo["sale_unit"] = "bulto"
                                    item_nuevo["is_promo"] = True
                                payload.append(item_nuevo)
                            else:
                                no_encontrados.append(nombre)
                print(f"[DIAG-GESTION] modificar payload={payload} | no_encontrados={no_encontrados}")
                if payload:
                    res_edit = await cpm_editar_items(tenant_id, oid, payload)
                    if res_edit.get("ok"):
                        texto = texto or f"Listo, actualicé tu pedido N° {objetivo.get('order_number', num_g)}."
                        gestion_completada = True
                        # Si lo agregado vino del carrito en curso, ya pasó al pedido: vaciarlo
                        # para que no quede huérfano ni genere un pedido nuevo.
                        agregados_nuevos = any((c.get("operacion") or "").lower() == "agregar" for c in cambios)
                        if carrito and agregados_nuevos:
                            carrito = []
                            pedido_registrado = {"ok": True, "via_gestion": True}
                        # Re-consultar el pedido actualizado: sirve para la imagen Y para el total real
                        ped_act = await cpm_consultar_pedido(tenant_id, oid)
                        items_act = (ped_act.get("items") if isinstance(ped_act, dict) else None) or []
                        items_img = _items_cpm_a_imagen(items_act)
                        print(f"[DIAG-GESTION-IMG] ped_act_keys={list(ped_act.keys()) if isinstance(ped_act, dict) else 'no-dict'} | items_recibidos={len(items_act)} | items_img={len(items_img)}")
                        # Total real del pedido tras el cambio (del CPM, no calculado)
                        total_act = ped_act.get("total") if isinstance(ped_act, dict) else None
                        if total_act is None and items_img:
                            total_act = sum(it["precio"] * it["cantidad"] for it in items_img)
                        # Armar texto con el resumen actualizado y el total
                        if items_img:
                            detalle = "\n".join(f"• {it['cantidad']}x {it['product_name']}" for it in items_img)
                            base = texto or f"Listo, actualicé tu pedido N° {objetivo.get('order_number', num_g)}."
                            texto = f"{base}\n\nQueda así:\n{detalle}"
                            if total_act is not None:
                                texto += f"\n\nTotal: ${float(total_act):,.0f}"
                            imagen_url = await generar_imagen_pedido(tenant_id, cfg, items_img)
                            print(f"[DIAG-GESTION] imagen actualizada='{imagen_url}' | total={total_act}")
                    else:
                        if res_edit.get("error"):
                            # Error de NEGOCIO (sin cupo de promo, no fraccionable, sin stock):
                            # comunicarlo tal cual y ofrecer alternativa.
                            texto = (f"No pude aplicar el cambio: {res_edit['error']} "
                                     f"¿Querés ajustar la cantidad o probamos otra cosa?")
                        else:
                            texto = "No pude aplicar el cambio desde acá. Te paso con el equipo."
                elif no_encontrados:
                    # No se pudo mapear ningún cambio: NO digas "listo" en falso.
                    prods = ", ".join(no_encontrados)
                    texto = (f"No encontré {prods} en el pedido N° {objetivo.get('order_number', num_g)} "
                             f"para modificarlo. ¿Me confirmás el nombre del producto tal como está en el pedido?")
                else:
                    texto = texto or "¿Qué querés cambiar del pedido? Decime el producto y la cantidad."
            else:
                texto = (f"Tu pedido N° {objetivo.get('order_number', num_g)} ya está {_estado_natural(estado)}, "
                         f"así que no puedo modificarlo. Si querés, armamos un pedido nuevo con lo que quieras sumar.")
        # acc_g == "consultar" o "nada": el texto del modelo ya trae la respuesta natural
    else:  # agente_humano
        raw = await llamar_claude(prompt_agente_humano(cfg), historial, max_tokens=200)

    if not raw:
        return None, None, {}, ""

    if agente not in ("pedido", "gestion"):
        texto, json_data = parsear_respuesta(raw)
    else:
        json_data = {}

    historial.append({"role": "assistant", "content": texto})

    # Persistir (incluye carrito actualizado)
    nueva_tarea = agente if agente in AGENTES_CONTENIDO else ""
    # si se registró el pedido, ya no hay tarea de pedido pendiente
    if pedido_registrado:
        nueva_tarea = ""
    # GESTIÓN es "pegajosa": mientras el cliente siga sobre el mismo pedido, se mantiene.
    # No la cerramos por completar una modificación (el cliente suele seguir editando),
    # ni por una cortesía intermedia ("perfecto", "gracias") que cae en charla/asesor.
    # Solo se cierra si el cliente arranca algo nuevo explícito o confirma cierre.
    if tarea == "gestion" and agente in ("gestion", "charla", "asesor"):
        m_low = (mensaje or "").lower()
        cierra_gestion = any(k in m_low for k in [
            "pedido nuevo", "otro pedido", "nuevo pedido", "chau", "nada mas", "nada más",
            "listo gracias", "eso es todo", "gracias nada", "no gracias"])
        nueva_tarea = "" if cierra_gestion else "gestion"
    campos_persist = {
        "historial": historial,
        "agente_activo": agente,
        "tarea_pendiente": nueva_tarea,
        "pedido_en_curso": carrito,
    }
    if marca_pedido:
        campos_persist["ultimo_pedido_fecha"] = marca_pedido[0]
        campos_persist["ultimo_pedido_num"] = marca_pedido[1]
    await upsert_conversacion(tenant_id, contact_id, campos_persist)
    await guardar_log(tenant_id, contact_id, agente, mensaje, texto)

    return agente, texto, json_data, imagen_url


def _respuesta_unificada(agente, texto, json_data, transcripcion="", imagen_pedido=""):
    jd = json_data or {}
    return {
        "respuesta": texto,
        "mensaje": texto,
        "agente": agente,
        "escalar": bool(jd.get("escalar", False)) if agente == "agente_humano" else False,
        "transcripcion": transcripcion or "",
        "imagen_pedido": imagen_pedido or "",
    }


# ─────────────────────────────────────────────
# ENDPOINT
# ─────────────────────────────────────────────

@app.get("/")
async def health():
    return {"status": "CPM activo — catálogo en vivo CPM + promos + venta fraccionada + gestión de pedidos + imágenes"}


@app.post("/orquestador")
async def orquestador(request: Request):
    body = await request.json()
    page_id = str(body.get("page_id", "")).strip()
    contact_id = str(body.get("contact_id", "")).strip()
    mensaje = body.get("mensaje_usuario", "") or ""
    mensaje_audio = (body.get("mensaje_audio", "") or "").strip()

    # ManyChat no deja vaciar un campo desde la UI; usamos un valor centinela.
    # Si mensaje_audio trae un marcador (none, vacio, -, etc.), lo tratamos como "sin audio".
    if mensaje_audio.lower() in ("none", "null", "vacio", "vacío", "-", "n/a", "na", ""):
        mensaje_audio = ""

    if not page_id:
        return JSONResponse(_respuesta_unificada("charla", "Falta configurar el page_id en el request.", {}))
    if not contact_id or (not mensaje and not mensaje_audio):
        return JSONResponse(_respuesta_unificada("charla", "No pude procesar tu mensaje. Intentá de nuevo.", {}))

    tenant = await resolver_tenant(page_id)
    if not tenant:
        return JSONResponse(_respuesta_unificada("charla", "No encontré la configuración de este negocio. Avisá al administrador.", {}))

    transcripcion = ""  # se llena solo si el mensaje fue un audio

    # Detección de media. REGLA CLARA para evitar cruces:
    # - El AUDIO viene SIEMPRE por el campo dedicado mensaje_audio. Es la única fuente de audio.
    # - mensaje_usuario es SOLO texto o imagen. Si trae una URL de audio, es contaminación
    #   de un turno anterior → la ignoramos (no la transcribimos, no la usamos como texto).
    url_media = ""
    tipo_media = "texto"

    if mensaje_audio and mensaje_audio.lower().startswith("http"):
        t = tipo_de_url(mensaje_audio)
        if t == "audio":
            url_media = mensaje_audio
            tipo_media = "audio"
        elif t == "imagen":
            # una imagen cayó en el campo de audio: la tratamos como imagen
            url_media = mensaje_audio
            tipo_media = "imagen"

    # Si no hubo media por el campo dedicado, evaluamos mensaje_usuario
    if tipo_media == "texto":
        t = tipo_de_url(mensaje)
        if t == "imagen":
            url_media = mensaje
            tipo_media = "imagen"
        elif t == "audio":
            # URL de audio en mensaje_usuario = contaminación de turno anterior → ignorar
            mensaje = ""

    # LOG DIAGNÓSTICO — borrar tras la prueba
    print(f"[DIAG] mensaje='{mensaje[:60]}' | mensaje_audio='{mensaje_audio[:60]}'")
    print(f"[DIAG] url_media='{url_media[:60]}' | tipo_media='{tipo_media}'")

    if tipo_media == "audio":
        transcripto = await transcribir_audio(url_media)
        if not transcripto:
            return JSONResponse(_respuesta_unificada("charla", "No pude escuchar bien el audio 🙉 ¿me lo escribís o lo mandás de nuevo?", {}))
        mensaje = transcripto
        transcripcion = transcripto

    elif tipo_media == "imagen":
        lista = await get_lista_liviana(tenant["tenant_id"])
        resultado = await leer_imagen(url_media, formato_lista_liviana(lista))
        if resultado["tipo"] == "pedido" and resultado["items"]:
            partes = [f"{it.get('cantidad', 1)} x {it.get('producto', '')}" for it in resultado["items"]]
            mensaje = "Quiero pedir lo de esta imagen: " + ", ".join(partes)
        elif resultado["tipo"] == "descripcion":
            return JSONResponse(_respuesta_unificada("charla",
                f"Vi tu imagen: {resultado['texto']}. ¿Querés que te arme un pedido con algo de esto? Contame qué necesitás.", {}))
        else:
            return JSONResponse(_respuesta_unificada("charla", "No pude abrir bien la imagen. ¿Me la mandás de nuevo o me escribís qué necesitás?", {}))

    # 'transcripcion' siempre lleva el mensaje del cliente EN TEXTO:
    # si fue audio, ya tiene la transcripción; si no, usamos el texto/mensaje resuelto.
    if not transcripcion:
        transcripcion = mensaje

    agente, texto, json_data, imagen_url = await manejar_turno(tenant, contact_id, mensaje)
    if texto is None:
        return JSONResponse(_respuesta_unificada("charla", "Tardé más de lo esperado. ¿Podés repetir tu mensaje?", {}, transcripcion))
    return JSONResponse(_respuesta_unificada(agente, texto, json_data, transcripcion, imagen_url))
