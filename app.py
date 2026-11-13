"""Agregador de notícias em tempo real. Veja o README.md para instruções."""
import hashlib, json, os, re, socket, sqlite3, threading, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from calendar import timegm
from html import unescape
from urllib.parse import urljoin, urlsplit, urlunsplit

import feedparser
from flask import Flask, Response, jsonify, request

BASE = os.path.dirname(os.path.abspath(__file__))
def _pasta_gravavel():
    for p in (os.environ.get("PASTA_DADOS"), BASE, "/tmp"):
        if p:
            try:
                os.makedirs(p, exist_ok=True)
                t = os.path.join(p, ".teste")
                open(t, "w").close(); os.remove(t)
                return p
            except Exception:
                continue
    return "/tmp"


DB = os.path.join(_pasta_gravavel(), "noticias.db")
INTERVALO = int(os.environ.get("INTERVALO_SEGUNDOS", "120"))   # checa as fontes a cada 2 min
NOME_SITE = os.environ.get("NOME_SITE", "Meu Portal de Notícias")
AGENTE = "PortalNoticiasBot/1.0 (agregador; respeita robots.txt)"

app = Flask(__name__)


class Conn(sqlite3.Connection):
    """Conexão que fecha sozinha ao sair do bloco 'with' (evita travar o banco)."""
    def __exit__(self, *a):
        try:
            return super().__exit__(*a)
        finally:
            self.close()


ESCRITA = threading.Lock()      # só uma gravação por vez


def db():
    c = sqlite3.connect(DB, timeout=60, factory=Conn)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=60000")
    return c


def iniciar_banco():
    with ESCRITA, db() as c:
        try:
            c.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:
            pass
        c.execute("""CREATE TABLE IF NOT EXISTS noticias(
            id INTEGER PRIMARY KEY AUTOINCREMENT, hash TEXT UNIQUE, hash_titulo TEXT,
            titulo TEXT, resumo TEXT, link TEXT, imagem TEXT, fonte TEXT,
            categoria TEXT, publicado INTEGER)""")
        try:
            c.execute("ALTER TABLE noticias ADD COLUMN tema TEXT")
        except sqlite3.OperationalError:
            pass
        c.execute("CREATE INDEX IF NOT EXISTS i_pub ON noticias(publicado DESC)")
        c.execute("CREATE TABLE IF NOT EXISTS controle(url TEXT PRIMARY KEY, etag TEXT, modified TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS estado(nome TEXT PRIMARY KEY, feed_url TEXT, tentativa INTEGER, ultima INTEGER, status TEXT)")


def limpar(txt, limite=300):
    txt = unescape(re.sub(r"<[^>]+>", " ", txt or ""))
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt if len(txt) <= limite else txt[:limite].rsplit(" ", 1)[0] + "…"


def link_limpo(url):
    p = urlsplit(url)
    return urlunsplit((p.scheme, p.netloc.lower(), p.path.rstrip("/"), "", ""))


def achar_imagem(e):
    for chave in ("media_thumbnail", "media_content"):
        for m in e.get(chave, []) or []:
            if m.get("url"):
                return m["url"]
    for l in e.get("links", []):
        if l.get("rel") == "enclosure" and str(l.get("type", "")).startswith("image"):
            return l.get("href")
    m = re.search(r'<img[^>]+src="([^"]+)"', e.get("summary", "") or "")
    return m.group(1) if m else None


import unicodedata

ULTIMA = {"t": None}      # hora da última checagem de fontes

TEMAS = {
 "Política": "governo president* lula bolsonaro congresso senado senador* camara deputad* eleic* eleitor* partido* ministr* stf supremo politic* golpe democracia planalto prefeit* governador* oposicao tse impeachment orcamento voto* esquerda direita ditadura sindicat* greve mst pt psol pl",
 "Internacional": "eua estados unidos trump china russia ucrania israel gaza palestin* guerra otan onu europa argentina venezuela cuba franca america latina africa oriente medio biden milei putin internacional imperialismo",
 "Sociedade": "racism* racial genero feminis* lgbt* indigen* quilomb* violencia direitos humanos educacao saude moradia favela desigualdade periferia meio ambiente clima amazonia policia justica trabalho previdencia salario",
 "Música": "music* musico* cantor* cantora show* album* banda* disco* festival rock samba mpb rap funk jazz forro sertanej* dj single turne clipe compositor* orquestra violao bossa nova hip hop",
 "Cinema": "filme* cinema* cineast* documentario* curta-metragem longa-metragem oscar cannes roteir* audiovisual atriz ator diretor* mubi",
 "Entretenimento": "serie series novela* celebridade* reality bbb famos* humor game* jogo* streaming netflix podcast tv televisao quadrinho* anime",
 "Artes": "arte artes artista* exposic* museu* galeria fotograf* escultur* pintur* instalac* bienal curador* teatro danca performance patrimonio acervo",
 "Literatura": "livro* literatur* escritor* escritora romance poesia poema* poet* autor* autora editora ensaio* resenha conto* leitura prosa",
 "Ciência": "ciencia* cientist* pesquisa* universo astronom* espaco nasa biolog* fisica quimica tecnologia inteligencia artificial ia algoritm* internet dinossauro* descobert* planeta cerebro",
}


def _norm(t):
    return unicodedata.normalize("NFKD", (t or "").lower()).encode("ascii", "ignore").decode()


def _compilar(lista):
    partes = []
    for k in lista.split():
        partes.append(r"\b" + re.escape(k[:-1]) if k.endswith("*") else r"\b" + re.escape(k) + r"\b")
    return re.compile("|".join(partes))


# palavras com espaço (ex.: "estados unidos") foram separadas acima; usamos listas com " | " para frases
FRASES = {"Internacional": ["estados unidos", "america latina", "oriente medio"],
          "Sociedade": ["direitos humanos", "meio ambiente"],
          "Música": ["bossa nova", "hip hop"],
          "Ciência": ["inteligencia artificial"]}
PADROES = {}
for _t, _l in TEMAS.items():
    _p = _compilar(_l)
    _f = [re.compile(r"\b" + re.escape(x) + r"\b") for x in FRASES.get(_t, [])]
    PADROES[_t] = (_p, _f)


def classificar(titulo, resumo, padrao=None):
    nt, nr = _norm(titulo), _norm(resumo)
    pont = {}
    for t, (p, fr) in PADROES.items():
        s = 2 * len(p.findall(nt)) + len(p.findall(nr))
        s += sum(2 * len(x.findall(nt)) + len(x.findall(nr)) for x in fr)
        if t == padrao:
            s += 1
        pont[t] = s
    melhor = max(pont.items(), key=lambda kv: (kv[1], kv[0] == padrao))
    return melhor[0] if melhor[1] > 0 else (padrao or "Sociedade")


def reclassificar():
    """Dá tema às notícias antigas que ainda não têm."""
    try:
        with open(os.path.join(BASE, "fontes.json"), encoding="utf-8") as fh:
            padrao = {f["nome"]: f.get("tema") for f in json.load(fh)}
    except Exception:
        padrao = {}
    with ESCRITA, db() as c:
        for r in c.execute("SELECT id,titulo,resumo,fonte FROM noticias WHERE tema IS NULL").fetchall():
            c.execute("UPDATE noticias SET tema=? WHERE id=?",
                      (classificar(r["titulo"], r["resumo"], padrao.get(r["fonte"])), r["id"]))


CAMINHOS = ["feed/", "feed", "rss", "rss.xml", "feed.xml", "atom.xml", "index.xml", "RSS"]
socket.setdefaulttimeout(25)


def baixar(url):
    req = urllib.request.Request(url, headers={"User-Agent": AGENTE, "Accept": "text/html,*/*"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.read().decode("utf-8", "ignore")


def feed_valido(url):
    try:
        return bool(feedparser.parse(url, agent=AGENTE).entries)
    except Exception:
        return False


def descobrir_feed(f):
    """Testa os endereços informados; se falhar, procura o feed na home do site."""
    cand = list(f.get("feeds", []))
    try:
        html = baixar(f["site"])
        for m in re.finditer(r"<link[^>]+>", html, re.I):
            tag = m.group(0)
            if re.search(r"application/(rss|atom)\+xml", tag, re.I):
                h = re.search(r"href=[\"']([^\"']+)", tag, re.I)
                if h and "comments" not in h.group(1):
                    cand.append(urljoin(f["site"], unescape(h.group(1))))
    except Exception:
        pass
    base = f["site"].split("/pt")[0].rstrip("/") + "/"
    cand += [base + c for c in CAMINHOS]
    vistos = set()
    for u in cand:
        if u not in vistos:
            vistos.add(u)
            if feed_valido(u):
                return u
    return None


def estado(nome, **campos):
    with ESCRITA, db() as c:
        c.execute("INSERT OR IGNORE INTO estado(nome) VALUES(?)", (nome,))
        for k, v in campos.items():
            c.execute(f"UPDATE estado SET {k}=? WHERE nome=?", (v, nome))


def coletar_fonte(f):
    agora = int(time.time())
    with db() as c:
        est = c.execute("SELECT * FROM estado WHERE nome=?", (f["nome"],)).fetchone()
    feed = est["feed_url"] if est else None
    if not feed:
        if est and est["tentativa"] and agora - est["tentativa"] < 3600:
            return 0                      # já tentou há pouco; tenta de novo em 1h
        estado(f["nome"], tentativa=agora)
        feed = descobrir_feed(f)
        if not feed:
            estado(f["nome"], status="SEM FEED encontrado", feed_url=None)
            return 0
        estado(f["nome"], feed_url=feed)
    with db() as c:
        ctl = c.execute("SELECT * FROM controle WHERE url=?", (feed,)).fetchone()
    d = feedparser.parse(feed, agent=AGENTE,
                         etag=ctl["etag"] if ctl else None,
                         modified=ctl["modified"] if ctl else None)
    if getattr(d, "status", 200) == 304:
        estado(f["nome"], status="OK", ultima=agora)
        return 0
    if not d.entries:
        estado(f["nome"], status="feed falhou (será redescoberto)", feed_url=None, tentativa=agora)
        return 0
    novas = 0
    with ESCRITA, db() as c:
        for e in d.entries[:40]:
            link, titulo = e.get("link"), limpar(e.get("title"), 250)
            if not link or not titulo:
                continue
            h = hashlib.sha1(link_limpo(link).encode()).hexdigest()
            ht = hashlib.sha1(re.sub(r"\W+", "", titulo.lower()).encode()).hexdigest()
            if c.execute("SELECT 1 FROM noticias WHERE hash=? OR hash_titulo=?", (h, ht)).fetchone():
                continue
            t = e.get("published_parsed") or e.get("updated_parsed")
            pub = min(timegm(t), agora) if t else agora
            cur = c.execute("""INSERT OR IGNORE INTO noticias(hash,hash_titulo,titulo,resumo,link,imagem,fonte,categoria,publicado,tema)
                         VALUES(?,?,?,?,?,?,?,?,?,?)""",
                      (h, ht, titulo, limpar(e.get("summary")), link, achar_imagem(e),
                       f["nome"], f.get("categoria", "Geral"), pub,
                       classificar(titulo, limpar(e.get("summary")), f.get("tema"))))
            novas += cur.rowcount
        c.execute("INSERT OR REPLACE INTO controle VALUES(?,?,?)",
                  (feed, getattr(d, "etag", None), getattr(d, "modified", None)))
    estado(f["nome"], status="OK", ultima=agora)
    return novas


def coletar_seguro(f):
    try:
        n = coletar_fonte(f)
        if n:
            print(f"[{f['nome']}] {n} nova(s)")
    except Exception as ex:
        estado(f["nome"], status=f"erro: {str(ex)[:80]}")
        print(f"[{f.get('nome')}] falhou: {ex}")
    ULTIMA["t"] = int(time.time())


def loop_coleta():
    while True:
        try:
            with open(os.path.join(BASE, "fontes.json"), encoding="utf-8") as fh:
                fontes = json.load(fh)
        except Exception as ex:
            print("Erro lendo fontes.json:", ex)
            fontes = []
        with ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(coletar_seguro, fontes))
        time.sleep(INTERVALO)


@app.route("/status")
def status():
    with db() as c:
        rows = c.execute("""SELECT e.nome, e.status, e.feed_url,
            (SELECT COUNT(*) FROM noticias n WHERE n.fonte=e.nome) total FROM estado e ORDER BY e.nome""").fetchall()
    linhas = "".join(f"<tr><td>{r['nome']}</td><td>{r['status'] or 'aguardando'}</td><td>{r['total']}</td>"
                     f"<td style='word-break:break-all'>{r['feed_url'] or ''}</td></tr>" for r in rows)
    return ("<meta charset=utf-8><title>Status</title><style>body{font:14px system-ui;margin:20px}"
            "td,th{border-bottom:1px solid #8884;padding:6px 10px;text-align:left}</style>"
            "<h2>Status das fontes</h2><table><tr><th>Fonte</th><th>Status</th><th>Notícias</th><th>Feed usado</th></tr>"
            + linhas + "</table>")


def linha_para_dict(r):
    return {k: r[k] for k in r.keys() if k not in ("hash", "hash_titulo")}


@app.errorhandler(Exception)
def erro_geral(ex):
    import traceback
    from werkzeug.exceptions import HTTPException
    if isinstance(ex, HTTPException):
        return ex
    traceback.print_exc()
    return (f"<meta charset=utf-8><h3>Erro no portal</h3><p>Copie esta linha e envie para quem cuida do portal:</p>"
            f"<pre>{type(ex).__name__}: {ex}</pre>"), 500


@app.route("/api/noticias")
def api_noticias():
    antes = int(request.args.get("antes", 9999999999))
    sql, p = "SELECT * FROM noticias WHERE publicado<?", [antes]
    for campo in ("tema", "fonte"):
        v = request.args.get(campo)
        if v:
            sql += f" AND {campo}=?"
            p.append(v)
    sql += " ORDER BY publicado DESC LIMIT 30"
    with db() as c:
        return jsonify([linha_para_dict(r) for r in c.execute(sql, p)])


@app.route("/api/filtros")
def api_filtros():
    tema, fonte = request.args.get("tema"), request.args.get("fonte")

    def cont(col, outro, valor):
        sql = f"SELECT {col} n, COUNT(*) t FROM noticias WHERE {col} IS NOT NULL"
        p = []
        if valor:
            sql += f" AND {outro}=?"
            p.append(valor)
        sql += f" GROUP BY {col} ORDER BY n"
        return [{"nome": r["n"], "total": r["t"]} for r in c.execute(sql, p)]

    with db() as c:
        temas, fontes = cont("tema", "fonte", fonte), cont("fonte", "tema", tema)
        ult = c.execute("SELECT MAX(ultima) FROM estado").fetchone()[0]
    return jsonify(temas=temas, fontes=fontes, atualizado=ULTIMA["t"] or ult)


@app.route("/stream")
def stream():
    def gerar():
        with db() as c:
            ultimo = c.execute("SELECT COALESCE(MAX(id),0) FROM noticias").fetchone()[0]
        while True:
            with db() as c:
                rows = c.execute("SELECT * FROM noticias WHERE id>? ORDER BY id", (ultimo,)).fetchall()
            for r in rows:
                ultimo = r["id"]
                yield f"data: {json.dumps(linha_para_dict(r), ensure_ascii=False)}\n\n"
            if not rows:
                yield ": ping\n\n"
            time.sleep(3)
    return Response(gerar(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


PAGINA = r"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>__NOME__</title>
<style>
:root{--bg:#f3ebdc;--card:#fffaf0;--tx:#1b1212;--mut:#6b5a58;--red:#b3121f;--red2:#7f0d16;--gold:#f2b705;--bd:#e2d6c0}
@media(prefers-color-scheme:dark){:root{--bg:#150909;--card:#211111;--tx:#f6ece0;--mut:#b9a49f;--red:#d62b32;--red2:#8e1119;--bd:#3a1d1d}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:16px/1.45 system-ui,Segoe UI,Arial,sans-serif}
.top{background:var(--red);color:#fff7e8;border-bottom:5px solid var(--gold)}
.wrap{max-width:1180px;margin:auto;padding:16px;display:flex;flex-wrap:wrap;align-items:baseline;gap:6px 18px}
h1{margin:0;font:900 1.9rem/1.1 Impact,"Arial Black",system-ui,sans-serif;letter-spacing:.04em;text-transform:uppercase}
.live{font-size:.82rem;font-weight:700;letter-spacing:.03em}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:var(--gold);margin-right:6px;animation:p 1.6s infinite}
@keyframes p{50%{opacity:.25}}
.layout{max-width:1180px;margin:auto;display:flex;gap:22px;padding:16px;align-items:flex-start}
aside{width:250px;flex:none;position:sticky;top:12px;max-height:calc(100vh - 24px);overflow-y:auto;background:var(--card);border:1px solid var(--bd);border-top:4px solid var(--red);border-radius:6px;padding:6px 10px 12px}
aside h3{margin:14px 0 6px;font:800 .78rem system-ui;letter-spacing:.12em;text-transform:uppercase;color:var(--red)}
aside ul{list-style:none;margin:0;padding:0}
aside button{width:100%;display:flex;align-items:center;gap:8px;text-align:left;border:0;background:none;color:var(--tx);padding:6px 8px;border-radius:4px;cursor:pointer;font:inherit;font-size:.9rem}
aside button:hover{background:var(--bd)}aside button.on{background:var(--red);color:#fff;font-weight:700}
aside .sq{width:11px;height:11px;border-radius:2px;flex:none}aside .n{margin-left:auto;font-size:.75rem;opacity:.7}
main{flex:1;min-width:0}main h2.t{margin:2px 0 8px;font:900 1.2rem Impact,"Arial Black",system-ui,sans-serif;letter-spacing:.04em;text-transform:uppercase;border-bottom:3px solid var(--red);padding-bottom:6px}
a.item{display:flex;gap:14px;background:var(--card);border:1px solid var(--bd);border-left:7px solid #888;border-radius:6px;padding:12px;margin:10px 0;text-decoration:none;color:inherit}
a.item:hover{border-color:var(--red)}
a.item img{width:130px;height:90px;object-fit:cover;border-radius:4px;flex:none}
.meta{font-size:.78rem;color:var(--mut);margin-bottom:3px}.meta b{color:var(--tx)}
.tag{display:inline-block;color:#fff;font-weight:800;font-size:.68rem;letter-spacing:.08em;text-transform:uppercase;padding:2px 8px;border-radius:3px;margin-right:6px}
h2.c{font-size:1.08rem;margin:2px 0 4px}p{margin:0;color:var(--mut);font-size:.9rem}
.novo{animation:f 1.5s}@keyframes f{from{background:#f2b70555}}
#mais{display:block;margin:20px auto;padding:10px 26px;border-radius:4px;border:2px solid var(--red);background:none;color:var(--red);font-weight:700;cursor:pointer}
#fb{display:none;margin:12px 16px 0;padding:8px 16px;border:2px solid var(--red);background:var(--card);color:var(--red);font-weight:700;border-radius:4px}
@media(max-width:800px){.layout{flex-direction:column;padding:10px}aside{display:none;width:100%;position:static;max-height:none}aside.open{display:block}#fb{display:inline-block}
 a.item img{width:90px;height:66px}h1{font-size:1.5rem}}
</style></head><body>
<div class="top"><div class="wrap"><h1>★ __NOME__</h1>
<div class="live"><span class="dot"></span>AO VIVO · Última atualização: <span id="upd">aguardando…</span></div></div></div>
<button id="fb">☰ Filtros</button>
<div class="layout"><aside id="side"><h3>Eixos temáticos</h3><ul id="temas"></ul><h3>Portais</h3><ul id="fontes"></ul></aside>
<main><h2 class="t" id="titulo">Todas as notícias</h2><div id="lista"></div><button id="mais">Carregar mais</button></main></div>
<script>
const CORES={"Política":"#c8102e","Internacional":"#1f4e5f","Sociedade":"#d9480f","Música":"#a87400","Cinema":"#6d28d9","Entretenimento":"#c2255c","Artes":"#2b8a3e","Literatura":"#7c4a1e","Ciência":"#1971c2"};
let tema="",fonte="",ultimo=null,timer=null;const $=id=>document.getElementById(id),L=$("lista");
function ago(t){const s=Math.max(0,Date.now()/1000-t);if(s<60)return"agora";if(s<3600)return"há "+Math.floor(s/60)+" min";
 if(s<86400)return"há "+Math.floor(s/3600)+" h";return new Date(t*1000).toLocaleDateString("pt-BR")}
function fmt(t){return new Date(t*1000).toLocaleString("pt-BR",{timeZone:"America/Sao_Paulo",day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"}).replace(", "," às ")}
function card(n,novo){const a=document.createElement("a");a.className="item"+(novo?" novo":"");a.href=n.link;a.target="_blank";a.rel="noopener";
 a.style.borderLeftColor=CORES[n.tema]||"#888";
 if(n.imagem){const i=document.createElement("img");i.src=n.imagem;i.loading="lazy";i.onerror=()=>i.remove();a.appendChild(i)}
 const d=document.createElement("div"),m=document.createElement("div");m.className="meta";
 const g=document.createElement("span");g.className="tag";g.style.background=CORES[n.tema]||"#666";g.textContent=n.tema||"Geral";
 const b=document.createElement("b");b.textContent=n.fonte;m.append(g,b," · "+ago(n.publicado));
 const h=document.createElement("h2");h.className="c";h.textContent=n.titulo;const p=document.createElement("p");p.textContent=n.resumo||"";
 d.append(m,h,p);a.appendChild(d);return a}
function qs(extra){const q=new URLSearchParams(extra||{});if(tema)q.set("tema",tema);if(fonte)q.set("fonte",fonte);return q}
function titulo(){$("titulo").textContent=(tema||fonte)?[tema,fonte].filter(Boolean).join(" · "):"Todas as notícias"}
async function carregar(reset){if(reset){L.innerHTML="";ultimo=null}titulo();
 const r=await (await fetch("/api/noticias?"+qs(ultimo?{antes:ultimo}:{}))).json();
 r.forEach(n=>{L.appendChild(card(n));ultimo=n.publicado});$("mais").style.display=r.length<30?"none":"block";
 if(reset&&!r.length)L.innerHTML="<p>Nenhuma notícia ainda neste filtro.</p>"}
function item(nome,total,on,cor,click){const li=document.createElement("li"),b=document.createElement("button");if(on)b.className="on";
 if(cor){const s=document.createElement("span");s.className="sq";s.style.background=cor;b.appendChild(s)}
 const t=document.createElement("span");t.textContent=nome;const n=document.createElement("span");n.className="n";n.textContent=total;
 b.append(t,n);b.onclick=()=>{click();$("side").classList.remove("open")};li.appendChild(b);return li}
async function filtros(){const r=await (await fetch("/api/filtros?"+qs())).json();
 const mp=Object.fromEntries(r.temas.map(x=>[x.nome,x.total])),T=$("temas"),F=$("fontes");T.innerHTML="";F.innerHTML="";
 T.appendChild(item("Todas",Object.values(mp).reduce((a,b)=>a+b,0),!tema,null,()=>{tema="";go()}));
 Object.keys(CORES).forEach(k=>T.appendChild(item(k,mp[k]||0,tema===k,CORES[k],()=>{tema=k;go()})));
 F.appendChild(item("Todos os portais",r.fontes.reduce((a,b)=>a+b.total,0),!fonte,null,()=>{fonte="";go()}));
 r.fontes.forEach(x=>F.appendChild(item(x.nome,x.total,fonte===x.nome,null,()=>{fonte=x.nome;go()})));
 $("upd").textContent=r.atualizado?fmt(r.atualizado):"aguardando a primeira coleta…"}
function go(){carregar(true);filtros()}
$("mais").onclick=()=>carregar(false);$("fb").onclick=()=>$("side").classList.toggle("open");
const es=new EventSource("/stream");
es.onmessage=e=>{const n=JSON.parse(e.data);if((!tema||tema===n.tema)&&(!fonte||fonte===n.fonte))L.prepend(card(n,true));
 clearTimeout(timer);timer=setTimeout(filtros,2000)};
go();setInterval(filtros,30000);
</script></body></html>
"""


@app.route("/")
def home():
    return PAGINA.replace("__NOME__", NOME_SITE)


iniciar_banco()
reclassificar()
threading.Thread(target=loop_coleta, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)
