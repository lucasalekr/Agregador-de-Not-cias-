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


ESCRITA = threading.RLock()      # só uma gravação por vez


def db(espera=60):
    c = sqlite3.connect(DB, timeout=espera, factory=Conn)
    c.row_factory = sqlite3.Row
    c.execute(f"PRAGMA busy_timeout={int(espera * 1000)}")
    return c


def iniciar_banco():
    with ESCRITA, db() as c:
        try:
            c.execute("PRAGMA journal_mode=DELETE")
        except sqlite3.Error:
            pass
        c.execute("""CREATE TABLE IF NOT EXISTS noticias(
            id INTEGER PRIMARY KEY AUTOINCREMENT, hash TEXT UNIQUE, hash_titulo TEXT,
            titulo TEXT, resumo TEXT, link TEXT, imagem TEXT, fonte TEXT,
            categoria TEXT, publicado INTEGER)""")
        for coluna in ("tema", "busca"):
            try:
                c.execute(f"ALTER TABLE noticias ADD COLUMN {coluna} TEXT")
            except sqlite3.OperationalError:
                pass
        c.execute("CREATE INDEX IF NOT EXISTS i_pub ON noticias(publicado DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS i_fonte ON noticias(fonte)")
        c.execute("CREATE INDEX IF NOT EXISTS i_tema ON noticias(tema)")
        c.execute("CREATE TABLE IF NOT EXISTS controle(url TEXT PRIMARY KEY, etag TEXT, modified TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS estado(nome TEXT PRIMARY KEY, feed_url TEXT, tentativa INTEGER, ultima INTEGER, status TEXT)")


def limpar(txt, limite=300):
    txt = unescape(re.sub(r"<[^>]+>", " ", (txt or "")[:6000]))
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
    m = re.search(r'<img[^>]+src="([^"]+)"', (e.get("summary", "") or "")[:4000])
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
        for r in c.execute("SELECT id,titulo,resumo,fonte FROM noticias WHERE busca IS NULL").fetchall():
            c.execute("UPDATE noticias SET busca=? WHERE id=?",
                      (_norm((r["titulo"] or "") + " " + (r["resumo"] or "") + " " + (r["fonte"] or "")), r["id"]))


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
        for e in d.entries[:25]:
            link, titulo = e.get("link"), limpar(e.get("title"), 250)
            if not link or not titulo:
                continue
            h = hashlib.sha1(link_limpo(link).encode()).hexdigest()
            ht = hashlib.sha1(re.sub(r"\W+", "", titulo.lower()).encode()).hexdigest()
            if c.execute("SELECT 1 FROM noticias WHERE hash=? OR hash_titulo=?", (h, ht)).fetchone():
                continue
            t = e.get("published_parsed") or e.get("updated_parsed")
            pub = min(timegm(t), agora) if t else agora
            cur = c.execute("""INSERT OR IGNORE INTO noticias(hash,hash_titulo,titulo,resumo,link,imagem,fonte,categoria,publicado,tema,busca)
                         VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                      (h, ht, titulo, limpar(e.get("summary")), link, achar_imagem(e),
                       f["nome"], f.get("categoria", "Geral"), pub,
                       classificar(titulo, limpar(e.get("summary")), f.get("tema")),
                       _norm(titulo + " " + limpar(e.get("summary")) + " " + f["nome"])))
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
        print(f"[{f.get('nome')}] falhou: {ex}")
        try:
            if banco_corrompido(ex):
                refazer_banco()
            else:
                estado(f["nome"], status=f"erro: {str(ex)[:80]}")
        except Exception:
            pass
    ULTIMA["t"] = int(time.time())
    time.sleep(1.5)        # pausa para o portal responder aos visitantes (plano gratuito tem pouca CPU)


def _loop_coleta_interno():
    while True:
        try:
            with open(os.path.join(BASE, "fontes.json"), encoding="utf-8") as fh:
                fontes = json.load(fh)
        except Exception as ex:
            print("Erro lendo fontes.json:", ex)
            fontes = []
        try:
            with ThreadPoolExecutor(max_workers=2) as ex:
                list(ex.map(coletar_seguro, fontes))
        except Exception as erro:
            print("Erro no ciclo de coleta:", erro)
        time.sleep(INTERVALO)


COLETOR = None
ULTIMO_ERRO = [None]
TRAVA_COLETOR = threading.Lock()


def _trava_de_arquivo():
    try:
        import fcntl
    except ImportError:          # Windows: sem trava, roda direto
        return None
    f = open(os.path.join(os.path.dirname(DB), ".coleta.lock"), "w")
    while True:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return f
        except OSError:
            time.sleep(30)


def loop_coleta():
    print("Coleta de notícias iniciada.", flush=True)
    try:
        trava = _trava_de_arquivo()      # garante uma única coleta, mesmo com vários processos
        _loop_coleta_interno()
    except BaseException:
        import traceback
        ULTIMO_ERRO[0] = traceback.format_exc()[-800:]
        print("Coleta parou:", ULTIMO_ERRO[0], flush=True)
        raise


def iniciar_coletor():
    """Liga a coleta se ela não estiver rodando (também a religa se tiver parado)."""
    global COLETOR
    with TRAVA_COLETOR:
        if COLETOR is None or not COLETOR.is_alive():
            COLETOR = threading.Thread(target=loop_coleta, daemon=True, name="coleta")
            COLETOR.start()


@app.before_request
def garantir_coleta():
    if COLETOR is None or not COLETOR.is_alive():
        iniciar_coletor()


@app.route("/saude")
def saude():
    ultima = ULTIMA["t"]
    if not ultima:
        try:
            with db(2) as c:
                ultima = c.execute("SELECT MAX(ultima) FROM estado").fetchone()[0]
        except Exception:
            pass
    return jsonify(portal="ok", coleta_ativa=bool(COLETOR and COLETOR.is_alive()),
                   ultima_checagem=ultima, agora=int(time.time()), ultimo_erro=ULTIMO_ERRO[0],
                   banco=os.path.basename(os.path.dirname(DB)) or DB)


@app.route("/status")
def status():
    with db(8) as c:
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


ULT_REFEITO = [0]


def banco_corrompido(ex):
    return isinstance(ex, sqlite3.DatabaseError) and any(
        k in str(ex).lower() for k in ("malformed", "not a database", "disk i/o"))


def refazer_banco():
    """Banco corrompido: apaga e recria vazio (as notícias voltam na próxima coleta)."""
    with ESCRITA:
        if time.time() - ULT_REFEITO[0] < 30:      # outra tarefa acabou de refazer
            return
        ULT_REFEITO[0] = time.time()
        print("Banco corrompido: recriando do zero.")
        for suf in ("", "-wal", "-shm", "-journal"):
            try:
                os.remove(DB + suf)
            except OSError:
                pass
        iniciar_banco()


def preparar_banco():
    try:
        if os.path.exists(DB):
            with sqlite3.connect(DB) as t:
                if t.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise sqlite3.DatabaseError("quick_check falhou")
            sqlite3.connect(DB).close()
        iniciar_banco()
        reclassificar()
    except sqlite3.DatabaseError:
        refazer_banco()


@app.errorhandler(Exception)
def erro_geral(ex):
    if banco_corrompido(ex):
        refazer_banco()
        return "<meta charset=utf-8><h3>O banco de dados foi recriado.</h3><p>Aguarde 1 ou 2 minutos e atualize a página: as notícias estão sendo buscadas de novo.</p>", 503
    import traceback
    from werkzeug.exceptions import HTTPException
    if isinstance(ex, HTTPException):
        return ex
    traceback.print_exc()
    return (f"<meta charset=utf-8><h3>Erro no portal</h3><p>Copie esta linha e envie para quem cuida do portal:</p>"
            f"<pre>{type(ex).__name__}: {ex}</pre>"), 500


def _com_busca(sql, p):
    """Pesquisa sem diferenciar maiúsculas/acentos (cada palavra precisa aparecer)."""
    for w in _norm(request.args.get("q", "")).split()[:6]:
        w = re.sub(r"[%_\\]", "", w)
        if w:
            sql += " AND busca LIKE ?"
            p.append("%" + w + "%")
    return sql, p


@app.route("/api/noticias")
def api_noticias():
    antes = int(request.args.get("antes", 9999999999))
    sql, p = "SELECT * FROM noticias WHERE publicado<?", [antes]
    sql, p = _com_busca(sql, p)
    for campo in ("tema", "fonte"):
        v = request.args.get(campo)
        if v:
            sql += f" AND {campo}=?"
            p.append(v)
    sql += " ORDER BY publicado DESC LIMIT 30"
    with db(8) as c:
        return jsonify([linha_para_dict(r) for r in c.execute(sql, p)])


@app.route("/api/filtros")
def api_filtros():
    tema, fonte = request.args.get("tema"), request.args.get("fonte")

    def cont(col, outro, valor):
        sql = f"SELECT {col} n, COUNT(*) t FROM noticias WHERE {col} IS NOT NULL"
        p = []
        sql, p = _com_busca(sql, p)
        if valor:
            sql += f" AND {outro}=?"
            p.append(valor)
        sql += f" GROUP BY {col} ORDER BY n"
        return [{"nome": r["n"], "total": r["t"]} for r in c.execute(sql, p)]

    with db(8) as c:
        temas, fontes = cont("tema", "fonte", fonte), cont("fonte", "tema", tema)
        ult = c.execute("SELECT MAX(ultima) FROM estado").fetchone()[0]
    return jsonify(temas=temas, fontes=fontes, atualizado=ULTIMA["t"] or ult)


@app.route("/stream")
def stream():
    def gerar():
        with db() as c:
            ultimo = c.execute("SELECT COALESCE(MAX(id),0) FROM noticias").fetchone()[0]
        while True:
            try:
                with db() as c:
                    rows = c.execute("SELECT * FROM noticias WHERE id>? ORDER BY id", (ultimo,)).fetchall()
            except sqlite3.DatabaseError as e2:
                if banco_corrompido(e2):
                    refazer_banco()
                    ultimo = 0
                rows = []
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
:root{--bg:#f4f1ea;--card:#fbf9f4;--tx:#111;--mut:#5d5852;--red:#e0201f;--ln:#111}
@media(prefers-color-scheme:dark){:root{--bg:#0f0f0f;--card:#171717;--tx:#f4f1ea;--mut:#a39d94;--red:#ff4a3d;--ln:#4a4a4a}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:16px/1.4 "Helvetica Neue",Helvetica,Arial,sans-serif}
.hero{position:relative;overflow:hidden;border-bottom:2px solid var(--ln);color:var(--tx)}
.art{position:absolute;inset:0;width:100%;height:100%;z-index:0}
.hw{position:relative;z-index:1;max-width:1320px;margin:auto;padding:22px 24px 26px}
.kick{display:flex;flex-wrap:wrap;gap:4px 14px;font-size:.72rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:var(--red);margin-right:7px;animation:p 1.6s infinite}@keyframes p{50%{opacity:.25}}
h1{margin:34px 0 26px;max-width:11em;font-weight:900;font-size:clamp(2.4rem,7.2vw,5.6rem);line-height:.92;letter-spacing:-.045em}
h1 .st{color:var(--red)}
.search{display:flex;max-width:640px;border:2px solid var(--ln);background:var(--bg)}
.search input{flex:1;min-width:0;border:0;background:none;color:inherit;font:inherit;font-size:1.05rem;padding:13px 14px;outline:none}
.search button{border:0;border-left:2px solid var(--ln);background:var(--red);color:#fff;font-weight:800;padding:0 18px;cursor:pointer;font-size:.8rem;letter-spacing:.1em;text-transform:uppercase}
.layout{max-width:1320px;margin:auto;display:flex;gap:28px;padding:22px 24px 50px;align-items:flex-start}
aside{width:236px;flex:none;position:sticky;top:14px;max-height:calc(100vh - 28px);overflow-y:auto}
aside h3{margin:0 0 6px;padding-top:8px;border-top:2px solid var(--ln);font-size:.7rem;letter-spacing:.14em;text-transform:uppercase}
aside ul{list-style:none;margin:0 0 24px;padding:0}
aside button{width:100%;display:flex;align-items:center;gap:9px;text-align:left;border:0;border-bottom:1px solid var(--ln);background:none;color:var(--tx);padding:7px 6px;cursor:pointer;font:inherit;font-size:.88rem}
aside button:hover{color:var(--red)}aside button.on{background:var(--red);color:#fff;font-weight:800}
aside .sq{width:10px;height:10px;flex:none}aside .n{margin-left:auto;font-size:.74rem;font-variant-numeric:tabular-nums;opacity:.7}
main{flex:1;min-width:0}
.bar{display:flex;justify-content:space-between;align-items:baseline;gap:12px;padding-bottom:8px;border-top:2px solid var(--ln);padding-top:8px}
.bar h2{margin:0;font-size:1.05rem;font-weight:900;letter-spacing:-.01em}.bar span{font-size:.72rem;letter-spacing:.1em;text-transform:uppercase;color:var(--mut)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));border-top:1px solid var(--ln);border-left:1px solid var(--ln)}
a.card{display:flex;flex-direction:column;background:var(--card);color:inherit;text-decoration:none;border-right:1px solid var(--ln);border-bottom:1px solid var(--ln)}
.im{aspect-ratio:16/10;overflow:hidden;background:#ccc;border-bottom:1px solid var(--ln)}
.im img{width:100%;height:100%;object-fit:cover;display:block;transition:transform .4s}a.card:hover .im img{transform:scale(1.04)}
.ph{width:100%;height:100%;background:radial-gradient(circle at 28% 30%,rgba(255,255,255,.55),transparent 42%),radial-gradient(circle at 70% 75%,rgba(0,0,0,.35),transparent 50%),var(--c)}
.cb{padding:12px 14px 14px;display:flex;flex-direction:column;gap:7px;flex:1}
.row{display:flex;justify-content:space-between;gap:10px;font-size:.66rem;font-weight:800;letter-spacing:.1em;text-transform:uppercase}
.tag{display:inline-flex;align-items:center;gap:6px;white-space:nowrap}.tag i{width:10px;height:10px;background:var(--c);display:inline-block}
.src{color:var(--mut);text-align:right}
a.card h3{margin:0;font-size:1.12rem;line-height:1.12;letter-spacing:-.02em;font-weight:800}a.card:hover h3{color:var(--red)}
a.card p{margin:0;font-size:.85rem;line-height:1.35;color:var(--mut);display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
a.card time{margin-top:auto;padding-top:8px;font-size:.66rem;letter-spacing:.1em;text-transform:uppercase;color:var(--mut)}
.novo{animation:f 1.6s}@keyframes f{from{background:rgba(224,32,31,.25)}}
.vazio{padding:30px 14px;color:var(--mut)}
#mais{display:block;margin:24px auto 0;padding:12px 30px;border:2px solid var(--ln);background:none;color:var(--tx);font-weight:800;letter-spacing:.1em;text-transform:uppercase;font-size:.78rem;cursor:pointer}
#mais:hover{background:var(--red);color:#fff;border-color:var(--red)}
#fb{display:none;margin:14px 24px 0;padding:9px 16px;border:2px solid var(--ln);background:none;color:var(--tx);font-weight:800;letter-spacing:.08em;text-transform:uppercase;font-size:.75rem}
@media(max-width:860px){.layout{flex-direction:column;padding:14px}aside{display:none;width:100%;position:static;max-height:none}aside.open{display:block}#fb{display:inline-block}.hw{padding:18px 16px 22px}.grid{grid-template-columns:repeat(auto-fill,minmax(200px,1fr))}}
</style></head><body>
<header class="hero">
<svg class="art" viewBox="0 0 1200 470" preserveAspectRatio="xMaxYMid slice" aria-hidden="true">
<defs>
<radialGradient id="sp" cx=".34" cy=".3" r=".9"><stop offset="0" stop-color="#ffc2cf"/><stop offset=".42" stop-color="#f2506f"/><stop offset="1" stop-color="#b3121f"/></radialGradient>
<filter id="gr" x="-15%" y="-15%" width="130%" height="130%"><feGaussianBlur in="SourceGraphic" stdDeviation="2.2" result="b"/>
<feTurbulence type="fractalNoise" baseFrequency=".9" numOctaves="3" seed="7" result="n"/>
<feColorMatrix in="n" type="matrix" values="0 0 0 0 .1  0 0 0 0 0  0 0 0 0 0  0 0 0 1.5 -.62" result="d"/>
<feComposite in="d" in2="b" operator="in" result="dd"/><feMerge><feMergeNode in="b"/><feMergeNode in="dd"/></feMerge></filter>
<filter id="air" x="-40%" y="-40%" width="180%" height="180%"><feGaussianBlur stdDeviation="16"/></filter>
</defs>
<circle cx="1010" cy="225" r="205" fill="url(#sp)" filter="url(#gr)"/>
<path d="M1200 470 L1200 190 A280 280 0 0 0 920 470 Z" fill="#e0201f" opacity=".55" filter="url(#air)"/>
<circle cx="1130" cy="400" r="95" fill="currentColor" opacity=".85" filter="url(#air)"/>
<circle cx="760" cy="70" r="46" fill="#ff7a59" opacity=".8" filter="url(#air)"/>
<circle cx="1010" cy="225" r="262" fill="none" stroke="currentColor" stroke-width="1.4"/>
<circle cx="1010" cy="225" r="318" fill="none" stroke="currentColor" stroke-width="1.4" opacity=".6"/>
<line x1="640" y1="0" x2="640" y2="470" stroke="currentColor" stroke-width="1.4" opacity=".5"/>
</svg>
<div class="hw">
<div class="kick"><span><span class="dot"></span>Ao vivo</span><span>Última atualização: <span id="upd">aguardando…</span></span></div>
<h1><span class="st">★</span> __NOME__</h1>
<form class="search" id="f" onsubmit="return false"><input id="q" type="search" placeholder="Pesquisar notícias, temas ou portais…" autocomplete="off" aria-label="Pesquisar"><button type="button" id="x">Limpar</button></form>
</div></header>
<button id="fb">☰ Filtros</button>
<div class="layout"><aside id="side"><h3>Eixos temáticos</h3><ul id="temas"></ul><h3>Portais</h3><ul id="fontes"></ul></aside>
<main><div class="bar"><h2 id="titulo">Todas as notícias</h2><span id="cont"></span></div><div class="grid" id="lista"></div><button id="mais">Carregar mais</button></main></div>
<script>
const CORES={"Política":"#e0201f","Internacional":"#1f4e5f","Sociedade":"#e8590c","Música":"#e0a100","Cinema":"#6d28d9","Entretenimento":"#d6336c","Artes":"#2b8a3e","Literatura":"#8a5a2b","Ciência":"#1971c2"};
let tema="",fonte="",q="",ultimo=null,timer=null,bt=null;const $=id=>document.getElementById(id),L=$("lista");
const norm=t=>(t||"").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"");
function ago(t){const s=Math.max(0,Date.now()/1000-t);if(s<60)return"agora";if(s<3600)return"há "+Math.floor(s/60)+" min";
 if(s<86400)return"há "+Math.floor(s/3600)+" h";return new Date(t*1000).toLocaleDateString("pt-BR")}
function fmt(t){return new Date(t*1000).toLocaleString("pt-BR",{timeZone:"America/Sao_Paulo",day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"}).replace(", "," às ")}
function card(n,novo){const a=document.createElement("a");a.className="card"+(novo?" novo":"");a.href=n.link;a.target="_blank";a.rel="noopener";
 a.style.setProperty("--c",CORES[n.tema]||"#666");
 const im=document.createElement("div");im.className="im";
 const ph=()=>{im.innerHTML="";const p=document.createElement("div");p.className="ph";im.appendChild(p)};
 if(n.imagem){const i=document.createElement("img");i.src=n.imagem;i.loading="lazy";i.alt="";i.onerror=ph;im.appendChild(i)}else ph();
 const b=document.createElement("div");b.className="cb",r=document.createElement("div");r.className="row";
 const tg=document.createElement("span");tg.className="tag";tg.append(document.createElement("i"),n.tema||"Geral");
 const sr=document.createElement("span");sr.className="src";sr.textContent=n.fonte;r.append(tg,sr);
 const h=document.createElement("h3");h.textContent=n.titulo;b.append(r,h);
 if(n.resumo){const p=document.createElement("p");p.textContent=n.resumo;b.append(p)}
 const t=document.createElement("time");t.textContent="Atualizado "+ago(n.publicado);t.title=fmt(n.publicado);b.append(t);
 a.append(im,b);return a}
function qs(extra){const p=new URLSearchParams(extra||{});if(tema)p.set("tema",tema);if(fonte)p.set("fonte",fonte);if(q)p.set("q",q);return p}
function titulo(){const f=[tema,fonte].filter(Boolean).join(" · ");$("titulo").textContent=q?("Resultados para “"+q+"”"+(f?" · "+f:"")):(f||"Todas as notícias")}
async function carregar(reset){if(reset){L.innerHTML="";ultimo=null}titulo();
 const r=await (await fetch("/api/noticias?"+qs(ultimo?{antes:ultimo}:{}))).json();
 r.forEach(n=>{L.appendChild(card(n));ultimo=n.publicado});$("mais").style.display=r.length<30?"none":"block";
 if(reset&&!r.length){const d=document.createElement("div");d.className="vazio";d.textContent="Nenhuma notícia encontrada.";L.appendChild(d)}}
function item(nome,total,on,cor,click){const li=document.createElement("li"),b=document.createElement("button");if(on)b.className="on";
 if(cor){const s=document.createElement("span");s.className="sq";s.style.background=cor;b.appendChild(s)}
 const t=document.createElement("span");t.textContent=nome;const n=document.createElement("span");n.className="n";n.textContent=total;
 b.append(t,n);b.onclick=()=>{click();$("side").classList.remove("open")};li.appendChild(b);return li}
async function filtros(){const r=await (await fetch("/api/filtros?"+qs())).json();
 const mp=Object.fromEntries(r.temas.map(x=>[x.nome,x.total])),T=$("temas"),F=$("fontes");T.innerHTML="";F.innerHTML="";
 const totT=Object.values(mp).reduce((a,b)=>a+b,0),totF=r.fontes.reduce((a,b)=>a+b.total,0);
 T.appendChild(item("Todas",totT,!tema,null,()=>{tema="";go()}));
 Object.keys(CORES).forEach(k=>T.appendChild(item(k,mp[k]||0,tema===k,CORES[k],()=>{tema=k;go()})));
 F.appendChild(item("Todos os portais",totF,!fonte,null,()=>{fonte="";go()}));
 r.fontes.forEach(x=>F.appendChild(item(x.nome,x.total,fonte===x.nome,null,()=>{fonte=x.nome;go()})));
 const n=fonte?((r.fontes.find(x=>x.nome===fonte)||{}).total||0):totF;$("cont").textContent=n+(n===1?" notícia":" notícias");
 $("upd").textContent=r.atualizado?fmt(r.atualizado):"aguardando a primeira coleta…"}
function go(){carregar(true);filtros()}
$("mais").onclick=()=>carregar(false);$("fb").onclick=()=>$("side").classList.toggle("open");
$("q").oninput=()=>{clearTimeout(bt);bt=setTimeout(()=>{q=$("q").value.trim();go()},300)};
$("x").onclick=()=>{$("q").value="";q="";go()};
const es=new EventSource("/stream");
es.onmessage=e=>{const n=JSON.parse(e.data),h=norm(n.titulo+" "+(n.resumo||"")+" "+n.fonte);
 const ok=norm(q).split(/\s+/).filter(Boolean).every(w=>h.includes(w));
 if(ok&&(!tema||tema===n.tema)&&(!fonte||fonte===n.fonte)){const v=L.querySelector(".vazio");if(v)v.remove();L.prepend(card(n,true))}
 clearTimeout(timer);timer=setTimeout(filtros,2000)};
go();setInterval(filtros,30000);
</script></body></html>
"""


@app.route("/")
def home():
    return PAGINA.replace("__NOME__", NOME_SITE)


preparar_banco()
# A coleta NÃO liga aqui: só no processo que atende visitas (primeira requisição),
# para não haver duas coletas disputando o banco quando o servidor carrega o app em 2 processos.

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)
