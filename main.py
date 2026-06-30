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


async def get_categorias_con_destacados(tenant_id: str, por_categoria: int = 3) -> str:
    """Para consultas amplias: lista categorías con algunos productos destacados de cada una."""
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/products",
            headers=_headers(),
            params={"tenant_id": f"eq.{tenant_id}", "active": "eq.true",
                    "select": "name,product_categories(name)"}
        )
        data = r.json()
    if not isinstance(data, list) or not data:
        return "(catálogo vacío)"
    # Agrupar por categoría
    cats = {}
    for p in data:
        pc = p.get("product_categories")
        cat = pc.get("name", "Otros") if isinstance(pc, dict) else "Otros"
        cats.setdefault(cat, []).append(p.get("name", ""))
    bloques = []
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
- ASESOR: consulta sobre productos cuando NO hay un pedido en curso (qué hay, características, precios, disponibilidad, recomendaciones, "qué me conviene").
- PEDIDO: el contacto quiere comprar / agregar productos / armar o confirmar un pedido ("quiero 3 de esto", "agregá", "cerrá el pedido"). TAMBIÉN cae acá CUALQUIER pregunta cuando hay un pedido en curso: precio, total, "cuánto es", "cómo queda el pedido", resumen, sacar o cambiar items. Si hay pedido en curso, quedate en PEDIDO.
- CONTINUAR: responde a algo que se le venía preguntando (un dato, una cantidad, una confirmación "sí"/"dale", una dirección).
- CHARLA: saludo, cortesía, agradecimiento, sin intención concreta.
- AGENTE_HUMANO: SOLO si pide explícitamente hablar con una persona real.

REGLAS:
- Si hay un PEDIDO EN CURSO (carrito con productos o tarea de pedido), las preguntas sobre precio, total o el estado del pedido van a PEDIDO, NO a ASESOR. El agente de pedido tiene los precios y el carrito.
- Si hay TAREA EN CURSO y el contacto sigue el hilo → CONTINUAR o PEDIDO según corresponda.
- AGENTE_HUMANO solo ante pedido explícito de un humano. Nunca por las dudas.
- Ante duda: si hay pedido/tarea en curso, quedate ahí; si no, CHARLA.

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

Tu rol ahora: TOMAR EL PEDIDO. Se vende por bulto entero. Atendé con naturalidad e incitá sutilmente a sumar productos (en vez de "¿con eso estaría?", preguntá "¿qué más te llevás?").

CÓMO TRABAJÁS:
- Identificá qué productos quiere agregar el cliente, usando los nombres EXACTOS del catálogo de abajo.
- Si el cliente es ambiguo (ej. dice "27" que es un formato/ml, no una cantidad; o no aclara fragancia/variante), PREGUNTÁ para aclarar ANTES de agregar. Nunca asumas cantidades: si no dice cuántos, asumí 1 bulto pero aclaralo.
- NO agregues un producto que ya está en el carrito de nuevo. Si el cliente corrige la cantidad ("quería 1, no 2"), usá accion "reemplazar" para fijar la cantidad correcta, no "agregar".
- PRECIOS: si el cliente pregunta el precio o el total, DECÍSELO. Los precios están en el carrito de abajo. Nunca digas que "no tenés los precios" — sí los tenés. Informá el total cuando lo pidan.

CONFIRMACIÓN (importante):
- Cuando el cliente quiera cerrar, mostrá el resumen con el total y pedí confirmación EXPLÍCITA: "¿Confirmás el pedido?".
- Marcá accion "confirmar" SOLO si el cliente confirma de forma clara: "confirmo", "sí, cerrá", "dale cerralo", "está bien cerrá". 
- Si el cliente dice algo ambiguo como "si" mientras pregunta otra cosa (ej. "si, cuánto es?"), NO es una confirmación: respondé su pregunta y volvé a pedir confirmación explícita. Ante la duda, NO confirmes.

CATÁLOGO (nombres exactos):
{lista_txt}

CARRITO ACTUAL (con precios):
{carrito_txt}

Respondé SIEMPRE con texto al cliente + este JSON al final (el cliente NO ve el JSON):
---JSON---
{{"accion": "agregar|reemplazar|nada|confirmar", "items": [{{"producto": "Nombre exacto del catálogo", "cantidad": 1}}]}}
---FIN---
- accion "agregar": sumar productos NUEVOS al carrito (los que no estaban).
- accion "reemplazar": corregir la cantidad de un producto que YA está en el carrito (poné la cantidad final correcta).
- accion "nada": cuando solo charlás, aclarás dudas, informás precios o preguntás (items vacío).
- accion "confirmar": SOLO ante confirmación explícita y clara del cliente (items vacío).
- La cantidad es en bultos."""


def prompt_agente_humano(cfg: dict) -> str:
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
    """Trae producto + variante default (precio, stock, reserved, ids) para los nombres dados."""
    if not nombres:
        return []
    valores = ",".join('"' + n.replace('"', '') + '"' for n in nombres)
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"{SUPABASE_URL}/rest/v1/products",
            headers=_headers(),
            params={"tenant_id": f"eq.{tenant_id}", "active": "eq.true",
                    "name": f"in.({valores})",
                    "select": "id,name,product_variants(id,name,price,stock,reserved,is_default,active)"}
        )
        data = r.json()
    out = []
    if isinstance(data, list):
        for p in data:
            variantes = [v for v in (p.get("product_variants") or []) if v.get("active", True)]
            # variante default o la primera
            v = next((x for x in variantes if x.get("is_default")), variantes[0] if variantes else None)
            if not v:
                continue
            stock = v.get("stock") or 0
            reserved = v.get("reserved") or 0
            out.append({
                "product_id": p["id"],
                "product_name": p["name"],
                "variant_id": v["id"],
                "precio": float(v.get("price") or 0),
                "disponible": max(0, stock - reserved),
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


async def registrar_pedido(tenant_id: str, contact_uuid: str, items: list, direccion: str = "") -> dict:
    """Crea orders + order_items y reserva stock. Devuelve {ok, order_id} o {ok: False, error}."""
    from datetime import timedelta
    total = total_pedido(items)
    delivery = (datetime.utcnow() + timedelta(days=1)).date().isoformat()
    try:
        async with httpx.AsyncClient(timeout=20) as client:
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

            prompt_vision = f"""Sos un asistente que lee imágenes para tomar pedidos de una distribuidora. El cliente mandó una foto que puede ser: una factura/remito anterior, una nota escrita a mano, o una lista de productos.

Tu tarea: identificá qué PRODUCTOS y CANTIDADES aparecen en la imagen, y cruzalos con este catálogo (usá los nombres EXACTOS del catálogo):

CATÁLOGO:
{lista_catalogo}

Respondé SOLO con este JSON:
{{"tipo": "pedido" o "descripcion", "items": [{{"producto": "nombre exacto del catálogo", "cantidad": N}}], "texto": "qué ves en la imagen, breve"}}

- Si identificás productos que matchean el catálogo → tipo "pedido", completá items con nombres exactos y cantidades.
- Si la imagen no tiene productos claros, está borrosa o no se entiende → tipo "descripcion", items vacío, y en "texto" describí brevemente qué ves.
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


RUTAS_VALIDAS = ("ASESOR", "PEDIDO", "CONTINUAR", "CHARLA", "AGENTE_HUMANO")
AGENTES_CONTENIDO = ("asesor", "pedido", "agente_humano")


async def clasificar_ruta(cfg: dict, historial: list, mensaje: str, tarea: str, hay_carrito: bool = False) -> str:
    hist = historial[-6:] if len(historial) > 6 else historial
    contexto = ""
    if hay_carrito:
        contexto = "[CONTEXTO: hay un PEDIDO EN CURSO con productos en el carrito. Cualquier pregunta sobre precio, total, resumen o cambios al pedido es PEDIDO, no ASESOR.]"
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
    elif ruta == "AGENTE_HUMANO":
        agente = "agente_humano"
    else:
        agente = "charla"

    # Si hay carrito en curso y cae en asesor/charla, forzar pedido (red de seguridad)
    if hay_carrito and agente in ("asesor", "charla"):
        agente = "pedido"

    # Carrito actual (lo necesita el agente pedido)
    carrito = carrito_previo
    pedido_registrado = None

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
        raw = await llamar_claude(
            prompt_pedido(cfg, formato_lista_liviana(lista), carrito_txt),
            historial, max_tokens=600
        )
        texto_tmp, jd_ped = parsear_respuesta(raw)
        accion = (jd_ped.get("accion") or "nada").lower()

        if accion in ("agregar", "reemplazar"):
            items_ped = jd_ped.get("items") or []
            nombres = [it.get("producto", "") for it in items_ped]
            cants = {it.get("producto", ""): int(it.get("cantidad", 1) or 1) for it in items_ped}
            encontrados = await buscar_producto_para_pedido(tenant_id, nombres)
            avisos = []
            for prod in encontrados:
                pedido_cant = cants.get(prod["product_name"], 1)
                if prod["disponible"] <= 0:
                    avisos.append(f"⚠️ {prod['product_name']}: sin stock, no lo pude agregar.")
                    continue
                if pedido_cant > prod["disponible"]:
                    avisos.append(f"⚠️ {prod['product_name']}: solo hay {prod['disponible']} disponibles, ajusté la cantidad.")
                    pedido_cant = prod["disponible"]
                # ¿ya está en el carrito?
                existente = next((c for c in carrito if c["product_id"] == prod["product_id"]), None)
                if existente:
                    if accion == "reemplazar":
                        existente["cantidad"] = pedido_cant   # fija la cantidad
                    else:
                        existente["cantidad"] += pedido_cant  # suma a lo que había
                else:
                    carrito.append({
                        "product_id": prod["product_id"],
                        "product_name": prod["product_name"],
                        "variant_id": prod["variant_id"],
                        "precio": prod["precio"],
                        "cantidad": pedido_cant,
                    })
            texto = texto_tmp
            if avisos:
                texto += "\n\n" + "\n".join(avisos)
            texto += "\n\n" + formato_tabla_pedido(carrito)

        elif accion == "confirmar":
            if not carrito:
                texto = "No tengo productos en el pedido todavía. ¿Qué te gustaría llevar?"
            else:
                res = await registrar_pedido(tenant_id, CONTACTO_PRUEBA, carrito, conv.get("direccion", ""))
                if res["ok"]:
                    pedido_registrado = res
                    texto = (f"{texto_tmp}\n\n✅ ¡Pedido registrado! Total: ${res['total']:,.0f}. "
                             f"Entrega prevista: {res['delivery']}. Te esperamos.")
                    carrito = []  # vaciar carrito tras registrar
                else:
                    texto = "Tuve un problema al registrar el pedido. ¿Probamos de nuevo en un momento?"
        else:
            texto = texto_tmp
    else:  # agente_humano
        raw = await llamar_claude(prompt_agente_humano(cfg), historial, max_tokens=200)

    if not raw:
        return None, None, {}

    if agente != "pedido":
        texto, json_data = parsear_respuesta(raw)
    else:
        json_data = {}

    historial.append({"role": "assistant", "content": texto})

    # Persistir (incluye carrito actualizado)
    nueva_tarea = agente if agente in AGENTES_CONTENIDO else ""
    # si se registró el pedido, ya no hay tarea de pedido pendiente
    if pedido_registrado:
        nueva_tarea = ""
    await upsert_conversacion(tenant_id, contact_id, {
        "historial": historial,
        "agente_activo": agente,
        "tarea_pendiente": nueva_tarea,
        "pedido_en_curso": carrito,
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
    return {"status": "CPM activo — multi-tenant + catálogo + pedidos + imágenes (Capa 1-5)"}


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

    # AUDIO: viene en su campo dedicado mensaje_audio (URL del archivo desde ManyChat)
    if mensaje_audio and mensaje_audio.lower().startswith("http"):
        transcripto = await transcribir_audio(mensaje_audio)
        if not transcripto:
            return JSONResponse(_respuesta_unificada("charla", "No pude escuchar bien el audio 🙉 ¿me lo escribís o lo mandás de nuevo?", {}))
        mensaje = transcripto
    else:
        # Si no hay audio, evaluar si mensaje_usuario es texto o imagen (URL)
        tipo_msg = tipo_de_url(mensaje)
        if tipo_msg == "imagen":
            lista = await get_lista_liviana(tenant["tenant_id"])
            resultado = await leer_imagen(mensaje, formato_lista_liviana(lista))
            if resultado["tipo"] == "pedido" and resultado["items"]:
                partes = [f"{it.get('cantidad', 1)} x {it.get('producto', '')}" for it in resultado["items"]]
                mensaje = "Quiero pedir lo de esta imagen: " + ", ".join(partes)
            elif resultado["tipo"] == "descripcion":
                return JSONResponse(_respuesta_unificada("charla",
                    f"Vi tu imagen: {resultado['texto']}. ¿Querés que te arme un pedido con algo de esto? Contame qué necesitás.", {}))
            else:
                return JSONResponse(_respuesta_unificada("charla", "No pude abrir bien la imagen. ¿Me la mandás de nuevo o me escribís qué necesitás?", {}))
        elif tipo_msg == "audio":
            # respaldo: audio que llegó por mensaje_usuario en vez del campo dedicado
            transcripto = await transcribir_audio(mensaje)
            if not transcripto:
                return JSONResponse(_respuesta_unificada("charla", "No pude escuchar bien el audio 🙉 ¿me lo escribís?", {}))
            mensaje = transcripto

    agente, texto, json_data = await manejar_turno(tenant, contact_id, mensaje)
    if texto is None:
        return JSONResponse(_respuesta_unificada("charla", "Tardé más de lo esperado. ¿Podés repetir tu mensaje?", {}))
    return JSONResponse(_respuesta_unificada(agente, texto, json_data))
