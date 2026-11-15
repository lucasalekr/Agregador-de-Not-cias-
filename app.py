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
        c.execute("CREATE TABLE IF NOT EXISTS acessos(tema TEXT PRIMARY KEY, total INTEGER DEFAULT 0)")
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


def baixar(url, tempo=25):
    req = urllib.request.Request(url, headers={"User-Agent": AGENTE, "Accept": "text/html,*/*"})
    with urllib.request.urlopen(req, timeout=tempo) as r:
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


def _lista(campo):
    return [v for v in request.args.get(campo, "").split(",") if v][:40]


RADIOS = {
    "fm": {"nome": "Rádio Nacional FM Brasília", "freq": "96,1 MHz", "slug": "radio-nacional-fm-de-brasilia", "env": "RADIO_FM_URL"},
    "am": {"nome": "Rádio Nacional AM Brasília", "freq": "980 kHz", "slug": "radio-nacional-am-de-brasilia", "env": "RADIO_AM_URL"},
}
EMBED_EBC = "https://aovivo.ebc.com.br/embed-audio.html?emissora="
_RE_STREAM = re.compile(r"https?://[^\s\"'<>\\)]+?\.(?:m3u8|mp3|aac|ogg|opus)(?:\?[^\s\"'<>\\)]*)?", re.I)
_CACHE_RADIO = {}


def _achar_streams(texto):
    return _RE_STREAM.findall((texto or "").replace("\\/", "/"))


def descobrir_stream(id_):
    """Procura, no player oficial da EBC, o endereço do áudio ao vivo da emissora."""
    r = RADIOS[id_]
    base = EMBED_EBC + r["slug"]
    achados = []
    try:
        html = baixar(base, 10)
    except Exception as ex:
        print("Rádio: não abriu o player da EBC:", ex)
        return None
    achados += _achar_streams(html)
    for src in re.findall(r"<script[^>]+src=[\"']([^\"']+)", html, re.I)[:4]:
        u = urljoin(base, src)
        if urlsplit(u).scheme == "https" and urlsplit(u).netloc.endswith("ebc.com.br"):
            try:
                achados += _achar_streams(baixar(u, 10)[:1500000])
            except Exception:
                pass
    achados = [u for u in dict.fromkeys(achados) if u.startswith("https://")]
    banda = id_                                   # "fm" ou "am"
    pref = [u for u in achados if re.search(r"brasilia|bsb", u, re.I)] or achados
    certos = [u for u in pref if re.search(r"(^|[^a-z])" + banda + r"([^a-z]|$)", u, re.I)]
    return (certos or pref or [None])[0]


@app.route("/api/radio/<id_>")
def api_radio(id_):
    if id_ not in RADIOS:
        return jsonify(erro="emissora desconhecida"), 404
    r = RADIOS[id_]
    agora = time.time()
    em_cache = _CACHE_RADIO.get(id_)
    if em_cache and agora < em_cache[0]:
        url = em_cache[1]
    else:
        url = os.environ.get(r["env"]) or descobrir_stream(id_)
        _CACHE_RADIO[id_] = (agora + (600 if url else 120), url)
    tipo = None if not url else ("hls" if ".m3u8" in url.lower() else "audio")
    return jsonify(nome=r["nome"], freq=r["freq"], stream=url, tipo=tipo, embed=EMBED_EBC + r["slug"])


@app.route("/api/clique", methods=["POST"])
def api_clique():
    """Conta cliques por tema para saber os temas mais acessados."""
    tema = (request.get_json(silent=True) or {}).get("tema")
    if tema in TEMAS:
        with ESCRITA, db(8) as c:
            c.execute("INSERT OR IGNORE INTO acessos(tema,total) VALUES(?,0)", (tema,))
            c.execute("UPDATE acessos SET total=total+1 WHERE tema=?", (tema,))
    return "", 204


@app.route("/api/noticias")
def api_noticias():
    antes = int(request.args.get("antes", 9999999999))
    sql, p = "SELECT * FROM noticias WHERE publicado<?", [antes]
    sql, p = _com_busca(sql, p)
    for campo in ("tema", "fonte"):
        vs = _lista(campo)
        if vs:
            sql += f" AND {campo} IN ({','.join('?' * len(vs))})"
            p += vs
    sql += " ORDER BY publicado DESC LIMIT 30"
    with db(8) as c:
        return jsonify([linha_para_dict(r) for r in c.execute(sql, p)])


@app.route("/api/filtros")
def api_filtros():
    tema, fonte = _lista("tema"), _lista("fonte")

    def cont(col, outro, valor):
        sql = f"SELECT {col} n, COUNT(*) t FROM noticias WHERE {col} IS NOT NULL"
        p = []
        sql, p = _com_busca(sql, p)
        if valor:
            sql += f" AND {outro} IN ({','.join('?' * len(valor))})"
            p += valor
        sql += f" GROUP BY {col} ORDER BY n"
        return [{"nome": r["n"], "total": r["t"]} for r in c.execute(sql, p)]

    with db(8) as c:
        temas, fontes = cont("tema", "fonte", fonte), cont("fonte", "tema", tema)
        ult = c.execute("SELECT MAX(ultima) FROM estado").fetchone()[0]
        pop = [r["tema"] for r in c.execute("SELECT tema FROM acessos WHERE total>0 ORDER BY total DESC LIMIT 3")]
        for r in c.execute("SELECT tema FROM noticias WHERE tema IS NOT NULL GROUP BY tema ORDER BY COUNT(*) DESC"):
            if len(pop) >= 3:
                break
            if r["tema"] not in pop:
                pop.append(r["tema"])
    return jsonify(temas=temas, fontes=fontes, atualizado=ULTIMA["t"] or ult, populares=pop)


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
:root{--bg:#fcf3f1;--tx:#1b1215;--mut:#6c5b60;--red:#e0201f;--glass:rgba(255,255,255,.44);--edge:rgba(255,255,255,.5);--sh:rgba(160,30,55,.24);--pnl:rgba(255,255,255,.86);--b1:.5;--b2:.32;--b3:.42;--b4:.28}
@media(prefers-color-scheme:dark){:root{--bg:#120a0d;--tx:#f8edef;--mut:#b6a3a8;--red:#ff4a3d;--glass:rgba(255,255,255,.07);--edge:rgba(255,255,255,.11);--sh:rgba(0,0,0,.55);--pnl:rgba(34,22,27,.88);--b1:.28;--b2:.22;--b3:.2;--b4:.16}}
*{box-sizing:border-box}html{background:var(--bg)}
body{margin:0;color:var(--tx);font:16px/1.4 "SF Pro Display","Inter","Segoe UI",system-ui,"Helvetica Neue",Arial,sans-serif;min-height:100vh}
body::before{content:"";position:fixed;inset:-12%;z-index:-2;filter:blur(36px);background:
radial-gradient(38% 34% at 10% 8%,rgba(255,110,135,var(--b1)),transparent 70%),
radial-gradient(34% 40% at 90% 16%,rgba(224,32,31,var(--b2)),transparent 70%),
radial-gradient(44% 40% at 72% 88%,rgba(255,165,115,var(--b3)),transparent 70%),
radial-gradient(40% 40% at 6% 82%,rgba(185,120,255,var(--b4)),transparent 70%),
radial-gradient(30% 30% at 48% 45%,rgba(255,190,200,calc(var(--b1) * .5)),transparent 70%)}
body::after{content:"";position:fixed;inset:0;z-index:-1;pointer-events:none;opacity:.07;background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='180' height='180'><filter id='n'><feTurbulence type='fractalNoise' baseFrequency='.9' numOctaves='2'/></filter><rect width='100%' height='100%' filter='url(%23n)'/></svg>")}
.hero{position:relative;overflow:hidden}
.art{position:absolute;inset:0;width:100%;height:100%;z-index:0;color:var(--tx)}
.hw{position:relative;z-index:1;max-width:1360px;margin:auto;padding:16px 28px 14px;display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:10px 28px}
.kick{display:flex;flex-wrap:wrap;gap:2px 14px;font-size:.68rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--mut)}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--red);margin-right:6px;animation:p 1.6s infinite}@keyframes p{50%{opacity:.25}}
h1{margin:4px 0 0;font-weight:800;font-size:clamp(1.5rem,3.2vw,2.4rem);line-height:1;letter-spacing:-.035em}h1 .st{color:var(--red)}
.search{display:flex;width:min(470px,100%);border-radius:99px;background:var(--glass);border:1px solid var(--edge);backdrop-filter:blur(18px) saturate(1.5);-webkit-backdrop-filter:blur(18px) saturate(1.5);box-shadow:0 10px 30px -12px var(--sh),inset 0 1px 0 rgba(255,255,255,.4);padding:4px}
.search input{flex:1;min-width:0;border:0;background:none;color:inherit;font:inherit;font-size:.98rem;padding:9px 16px;outline:none}
.search button{border:0;border-radius:99px;background:radial-gradient(ellipse at center,rgba(224,32,31,.95) 0%,rgba(224,32,31,.4) 100%);color:#fff;font-weight:700;padding:0 18px;cursor:pointer;font-size:.74rem;letter-spacing:.08em;text-transform:uppercase}
.layout{max-width:1360px;margin:auto;display:flex;gap:30px;padding:14px 28px 60px;align-items:flex-start}
aside{width:250px;flex:none;position:sticky;top:14px;padding:16px;border-radius:24px;background:var(--glass);border:1px solid var(--edge);backdrop-filter:blur(20px) saturate(1.5);-webkit-backdrop-filter:blur(20px) saturate(1.5);box-shadow:0 14px 40px -16px var(--sh),inset 0 1px 0 rgba(255,255,255,.4)}
aside h3{margin:0 0 7px;font-size:.66rem;letter-spacing:.14em;text-transform:uppercase;color:var(--mut)}
.dd{position:relative;margin-bottom:18px}.dd:last-child{margin-bottom:0}
.ddb{width:100%;display:flex;align-items:center;gap:8px;text-align:left;border:1px solid var(--edge);border-radius:16px;background:var(--glass);color:var(--tx);padding:10px 12px;cursor:pointer;font:inherit;font-size:.86rem;box-shadow:inset 0 1px 0 rgba(255,255,255,.35)}
.ddb .l{flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.ddb .c{opacity:.6;transition:transform .2s}.dd.open .c{transform:rotate(180deg)}
.ddp{display:none;position:absolute;left:0;right:0;top:calc(100% + 8px);z-index:30;padding:8px;border-radius:20px;background:var(--pnl);border:1px solid var(--edge);backdrop-filter:blur(26px) saturate(1.6);-webkit-backdrop-filter:blur(26px) saturate(1.6);box-shadow:0 24px 60px -12px var(--sh)}
.dd.open .ddp{display:block}
.acts{display:flex;gap:6px;padding:2px 2px 8px}.acts button{flex:1;border:0;border-radius:99px;padding:6px 8px;font-size:.7rem;font-weight:700;cursor:pointer;color:var(--tx);background:radial-gradient(ellipse at center,rgba(224,32,31,.22),rgba(224,32,31,.06))}
.lst{max-height:300px;overflow-y:auto}
.opt{display:flex;align-items:center;gap:9px;padding:7px 8px;border-radius:12px;cursor:pointer;font-size:.86rem;user-select:none}.opt:hover{background:rgba(224,32,31,.1)}
.opt .k{width:17px;height:17px;flex:none;border-radius:6px;border:1.5px solid var(--mut);display:grid;place-items:center;font-size:.65rem;color:transparent}
.opt.on .k{border-color:transparent;color:#fff;background:radial-gradient(circle,var(--red) 35%,rgba(224,32,31,.55) 100%)}
.opt .t{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.opt .n{font-size:.72rem;opacity:.6;font-variant-numeric:tabular-nums}
main{flex:1;min-width:0}
.bar{display:flex;justify-content:space-between;align-items:baseline;gap:12px;margin:2px 4px 18px}
.bar h2{margin:0;font-size:1.1rem;font-weight:800;letter-spacing:-.02em}.bar span{font-size:.7rem;letter-spacing:.1em;text-transform:uppercase;color:var(--mut);white-space:nowrap}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(262px,1fr));gap:30px}
a.card{position:relative;display:flex;flex-direction:column;gap:12px;padding:10px 10px 16px;border-radius:26px;overflow:hidden;text-decoration:none;color:inherit;background:var(--glass);border:1px solid var(--edge);backdrop-filter:blur(20px) saturate(1.5);-webkit-backdrop-filter:blur(20px) saturate(1.5);box-shadow:0 16px 44px -18px var(--sh),inset 0 1px 0 rgba(255,255,255,.45);transition:transform .3s,box-shadow .3s}
a.card::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(110% 70% at 0% 0%,var(--c0) 0%,transparent 62%);opacity:.55}
a.card>*{position:relative}a.card:hover{transform:translateY(-4px);box-shadow:0 26px 60px -16px var(--sh),inset 0 1px 0 rgba(255,255,255,.55)}
.im{aspect-ratio:16/10;border-radius:18px;overflow:hidden}.im img{width:100%;height:100%;object-fit:cover;display:block;transition:transform .5s}a.card:hover .im img{transform:scale(1.05)}
.ph{width:100%;height:100%;display:grid;place-items:center;font-size:2.8rem;background:radial-gradient(circle at 50% 50%,var(--c0) 0%,var(--c1) 100%)}
.cb{display:flex;flex-direction:column;gap:8px;padding:0 6px;flex:1}
.row{display:flex;justify-content:space-between;align-items:center;gap:10px}
.tag{display:inline-flex;align-items:center;gap:5px;padding:4px 11px;border-radius:99px;font-size:.68rem;font-weight:700;letter-spacing:.04em;white-space:nowrap;background:radial-gradient(ellipse at 50% 50%,var(--c0) 0%,var(--c1) 100%)}
.src{font-size:.7rem;font-weight:600;color:var(--mut);text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
a.card h3{margin:0;font-size:1.1rem;line-height:1.15;letter-spacing:-.02em;font-weight:750}
a.card p{margin:0;font-size:.85rem;line-height:1.38;color:var(--mut);display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
a.card time{margin-top:auto;padding-top:6px;font-size:.68rem;letter-spacing:.08em;text-transform:uppercase;color:var(--mut)}
.novo{animation:f 1.8s}@keyframes f{from{box-shadow:0 0 0 3px rgba(224,32,31,.55)}}
.vazio{padding:30px 6px;color:var(--mut)}
#mais{display:block;margin:34px auto 0;padding:12px 32px;border:1px solid var(--edge);border-radius:99px;background:var(--glass);backdrop-filter:blur(16px);color:var(--tx);font-weight:700;letter-spacing:.08em;text-transform:uppercase;font-size:.74rem;cursor:pointer}
#mais:hover{background:radial-gradient(ellipse at center,rgba(224,32,31,.9),rgba(224,32,31,.45));color:#fff}
#fb{display:none;margin:6px 28px 0;padding:9px 18px;border:1px solid var(--edge);border-radius:99px;background:var(--glass);color:var(--tx);font-weight:700;font-size:.78rem}
@media(max-width:860px){.layout{flex-direction:column;padding:12px 16px 50px;gap:18px}aside{display:none;width:100%;position:static}aside.open{display:block}#fb{display:inline-block;margin-left:16px}.hw{padding:14px 16px 10px}.grid{gap:20px;grid-template-columns:repeat(auto-fill,minmax(220px,1fr))}}
body{padding-bottom:118px}
.radio{position:fixed;left:50%;bottom:14px;transform:translateX(-50%);z-index:50;width:min(940px,calc(100% - 24px));display:flex;flex-wrap:wrap;align-items:center;gap:10px 16px;padding:10px 16px;border-radius:28px;background:var(--pnl);border:1px solid var(--edge);backdrop-filter:blur(26px) saturate(1.6);-webkit-backdrop-filter:blur(26px) saturate(1.6);box-shadow:0 22px 54px -14px var(--sh),inset 0 1px 0 rgba(255,255,255,.45)}
.rinfo{display:flex;align-items:center;gap:11px;flex:1 1 230px;min-width:0}
.rdot{width:11px;height:11px;border-radius:50%;flex:none;background:radial-gradient(circle,var(--red) 30%,rgba(224,32,31,.25) 100%)}.radio.on .rdot{animation:p 1.4s infinite}
.rinfo b{display:block;font-size:.9rem;letter-spacing:-.01em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.rinfo small{display:block;color:var(--mut);font-size:.7rem;letter-spacing:.08em;text-transform:uppercase}
.rband{display:flex;gap:4px;padding:3px;border-radius:99px;background:rgba(224,32,31,.09)}
.rband button{border:0;border-radius:99px;padding:6px 13px;font-size:.72rem;font-weight:700;background:none;color:var(--tx);cursor:pointer}
.rband button.on{background:radial-gradient(ellipse at center,rgba(224,32,31,.95),rgba(224,32,31,.5));color:#fff}
.rctl{display:flex;align-items:center;gap:10px}
#rplay{width:46px;height:46px;border-radius:50%;border:0;color:#fff;font-size:1rem;cursor:pointer;background:radial-gradient(circle at 35% 30%,#ff7a8c,rgba(224,32,31,.95) 60%,rgba(179,18,31,.72));box-shadow:0 8px 22px -6px rgba(224,32,31,.75)}
#rmute{border:0;background:none;font-size:1.3rem;cursor:pointer;padding:4px;line-height:1}
#rvol{width:120px;accent-color:var(--red)}
#rmsg{flex-basis:100%;font-size:.76rem;color:var(--mut)}#rmsg:empty{display:none}
#rlink{display:none;flex-basis:100%;font-size:.76rem;color:var(--red)}.radio.fb #rlink{display:block}
#rfb{flex-basis:100%;height:96px;border:0;border-radius:16px}#rfb[hidden]{display:none}
@media(max-width:620px){#rvol{width:84px}.rband{order:3}.radio{border-radius:22px;padding:10px 12px}}
</style></head><body>
<header class="hero">
<svg class="art" viewBox="0 0 1200 140" preserveAspectRatio="xMaxYMid slice" aria-hidden="true">
<defs>
<radialGradient id="sp" cx=".34" cy=".3" r=".9"><stop offset="0" stop-color="#ffc2cf"/><stop offset=".45" stop-color="#f2506f" stop-opacity=".9"/><stop offset="1" stop-color="#b3121f" stop-opacity=".22"/></radialGradient>
<filter id="gr" x="-15%" y="-15%" width="130%" height="130%"><feGaussianBlur in="SourceGraphic" stdDeviation="2" result="b"/>
<feTurbulence type="fractalNoise" baseFrequency=".9" numOctaves="3" seed="7" result="n"/>
<feColorMatrix in="n" type="matrix" values="0 0 0 0 .1  0 0 0 0 0  0 0 0 0 0  0 0 0 1.5 -.62" result="d"/>
<feComposite in="d" in2="b" operator="in" result="dd"/><feMerge><feMergeNode in="b"/><feMergeNode in="dd"/></feMerge></filter>
<filter id="air" x="-60%" y="-60%" width="220%" height="220%"><feGaussianBlur stdDeviation="12"/></filter>
</defs>
<circle cx="1050" cy="70" r="92" fill="url(#sp)" filter="url(#gr)"/>
<path d="M1200 140 L1200 20 A130 130 0 0 0 1070 140 Z" fill="#e0201f" opacity=".45" filter="url(#air)"/>
<circle cx="1170" cy="128" r="46" fill="currentColor" opacity=".5" filter="url(#air)"/>
<circle cx="880" cy="22" r="30" fill="#ff7a59" opacity=".7" filter="url(#air)"/>
<circle cx="1050" cy="70" r="128" fill="none" stroke="currentColor" stroke-width="1" opacity=".28"/>
<circle cx="1050" cy="70" r="172" fill="none" stroke="currentColor" stroke-width="1" opacity=".16"/>
</svg>
<div class="hw"><div><div class="kick"><span><span class="dot"></span>Ao vivo</span><span>Última atualização: <span id="upd">aguardando…</span></span></div>
<h1><span class="st">★</span> __NOME__</h1></div>
<form class="search" onsubmit="return false"><input id="q" type="search" placeholder="Pesquisar notícias, temas ou portais…" autocomplete="off" aria-label="Pesquisar"><button type="button" id="x">Limpar</button></form></div>
</header>
<button id="fb">☰ Filtros</button>
<div class="layout"><aside id="side"><h3>Eixos temáticos</h3><div class="dd" id="ddt"></div><h3>Portais</h3><div class="dd" id="ddf"></div></aside>
<main><div class="bar"><h2 id="titulo">Todas as notícias</h2><span id="cont"></span></div><div class="grid" id="lista"></div><button id="mais">Carregar mais</button></main></div>
<div id="radio" class="radio" role="region" aria-label="Rádio Nacional ao vivo">
<div class="rinfo"><span class="rdot"></span><div><b id="rnome">Rádio Nacional FM Brasília</b><small id="rsub">96,1 MHz · ao vivo</small></div></div>
<div class="rband"><button type="button" data-r="fm" class="on">FM 96,1</button><button type="button" data-r="am">AM 980</button></div>
<div class="rctl"><button type="button" id="rplay" aria-label="Tocar">▶</button><button type="button" id="rmute" aria-label="Mudo">🔊</button><input id="rvol" type="range" min="0" max="100" value="70" aria-label="Volume"></div>
<div id="rmsg"></div><a id="rlink" href="https://radionacional.ebc.com.br" target="_blank" rel="noopener">Abrir o site da Rádio Nacional ↗</a>
<iframe id="rfb" hidden allow="autoplay" title="Rádio Nacional (player oficial da EBC)"></iframe>
</div>
<script>
const CORES={"Política":"#e0201f","Internacional":"#1f4e5f","Sociedade":"#e8590c","Música":"#e0a100","Cinema":"#6d28d9","Entretenimento":"#d6336c","Artes":"#2b8a3e","Literatura":"#8a5a2b","Ciência":"#1971c2"};
const ICO={"Política":"🏛️","Internacional":"🌍","Sociedade":"✊","Música":"🎵","Cinema":"🎬","Entretenimento":"🍿","Artes":"🎨","Literatura":"📚","Ciência":"🔬"};
let selT=new Set(),selF=new Set(),pop=[],q="",ultimo=null,timer=null,bt=null,aberto=null,dados=null;
const $=id=>document.getElementById(id),L=$("lista");
const norm=t=>(t||"").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"");
const rgba=(h,a)=>{const n=parseInt(h.slice(1),16);return"rgba("+(n>>16)+","+((n>>8)&255)+","+(n&255)+","+a+")"};
function ago(t){const s=Math.max(0,Date.now()/1000-t);if(s<60)return"agora";if(s<3600)return"há "+Math.floor(s/60)+" min";
 if(s<86400)return"há "+Math.floor(s/3600)+" h";return new Date(t*1000).toLocaleDateString("pt-BR")}
function fmt(t){return new Date(t*1000).toLocaleString("pt-BR",{timeZone:"America/Sao_Paulo",day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"}).replace(", "," às ")}
function card(n,novo){const a=document.createElement("a");a.className="card"+(novo?" novo":"");a.href=n.link;a.target="_blank";a.rel="noopener";
 const c=CORES[n.tema]||"#888";a.style.setProperty("--c0",rgba(c,.5));a.style.setProperty("--c1",rgba(c,.07));
 a.onclick=()=>{try{navigator.sendBeacon("/api/clique",new Blob([JSON.stringify({tema:n.tema})],{type:"application/json"}))}catch(e){}};
 const im=document.createElement("div");im.className="im";
 const ph=()=>{im.innerHTML="";const p=document.createElement("div");p.className="ph";p.textContent=ICO[n.tema]||"📰";im.appendChild(p)};
 if(n.imagem){const i=document.createElement("img");i.src=n.imagem;i.loading="lazy";i.alt="";i.onerror=ph;im.appendChild(i)}else ph();
 const b=document.createElement("div");b.className="cb";const r=document.createElement("div");r.className="row";
 const tg=document.createElement("span");tg.className="tag";tg.textContent=(ICO[n.tema]||"📰")+" "+(n.tema||"Geral");
 const sr=document.createElement("span");sr.className="src";sr.textContent=n.fonte;r.append(tg,sr);
 const h=document.createElement("h3");h.textContent=n.titulo;b.append(r,h);
 if(n.resumo){const p=document.createElement("p");p.textContent=n.resumo;b.append(p)}
 const t=document.createElement("time");t.textContent="Atualizado "+ago(n.publicado);t.title=fmt(n.publicado);b.append(t);
 a.append(im,b);return a}
function qs(extra){const p=new URLSearchParams(extra||{});if(selT.size)p.set("tema",[...selT].join(","));if(selF.size)p.set("fonte",[...selF].join(","));if(q)p.set("q",q);return p}
function titulo(){const f=[...[...selT].map(t=>(ICO[t]||"")+" "+t),...selF].join(" · ");$("titulo").textContent=q?("Resultados para “"+q+"”"+(f?" · "+f:"")):(f||"Todas as notícias")}
async function carregar(reset){if(reset){L.innerHTML="";ultimo=null}titulo();
 const r=await (await fetch("/api/noticias?"+qs(ultimo?{antes:ultimo}:{}))).json();
 r.forEach(n=>{L.appendChild(card(n));ultimo=n.publicado});$("mais").style.display=r.length<30?"none":"block";
 if(reset&&!r.length){const d=document.createElement("div");d.className="vazio";d.textContent="Nenhuma notícia encontrada com esses filtros.";L.appendChild(d)}}
function dd(id,itens,sel,rotulo,extras){const box=$(id),old=box.querySelector(".lst"),sc=old?old.scrollTop:0;box.innerHTML="";box.classList.toggle("open",aberto===id);
 const b=document.createElement("button");b.type="button";b.className="ddb";
 const l=document.createElement("span");l.className="l";const ar=[...sel];
 l.textContent=!ar.length?rotulo:(id==="ddt"?ar.map(x=>ICO[x]||"").join("")+"  "+ar.length+(ar.length===1?" tema":" temas"):(ar.length<=2?ar.join(", "):ar.length+" portais"));
 const c=document.createElement("span");c.className="c";c.textContent="▾";b.append(l,c);
 b.onclick=e=>{e.stopPropagation();aberto=aberto===id?null:id;dd2()};
 const p=document.createElement("div");p.className="ddp";p.onclick=e=>e.stopPropagation();
 const ac=document.createElement("div");ac.className="acts";
 extras.forEach(([nome,fn])=>{const x=document.createElement("button");x.type="button";x.textContent=nome;x.onclick=fn;ac.appendChild(x)});
 const ls=document.createElement("div");ls.className="lst";
 itens.forEach(it=>{const o=document.createElement("div");o.className="opt"+(sel.has(it.nome)?" on":"");
  const k=document.createElement("span");k.className="k";k.textContent="✓";const t=document.createElement("span");t.className="t";t.textContent=(it.ico?it.ico+" ":"")+it.nome;
  const n=document.createElement("span");n.className="n";n.textContent=it.total;o.append(k,t,n);
  o.onclick=()=>{sel.has(it.nome)?sel.delete(it.nome):sel.add(it.nome);go()};ls.appendChild(o)});
 p.append(ac,ls);box.append(b,p);ls.scrollTop=sc}
function dd2(){if(!dados)return;const mp=Object.fromEntries(dados.temas.map(x=>[x.nome,x.total]));
 dd("ddt",Object.keys(CORES).map(k=>({nome:k,ico:ICO[k],total:mp[k]||0})),selT,"Todos os temas",[["Mais populares",()=>{selT=new Set(pop);go()}],["Ver todos",()=>{selT.clear();go()}]]);
 dd("ddf",dados.fontes,selF,"Todos os portais",[["Ver todos",()=>{selF.clear();go()}]])}
async function filtros(){dados=await (await fetch("/api/filtros?"+qs())).json();dd2();
 const tot=dados.fontes.reduce((a,b)=>a+b.total,0);$("cont").textContent=tot+(tot===1?" notícia":" notícias");
 $("upd").textContent=dados.atualizado?fmt(dados.atualizado):"aguardando a primeira coleta…"}
function go(){carregar(true);filtros()}
document.addEventListener("click",()=>{if(aberto){aberto=null;dd2()}});
$("mais").onclick=()=>carregar(false);$("fb").onclick=()=>$("side").classList.toggle("open");
$("q").oninput=()=>{clearTimeout(bt);bt=setTimeout(()=>{q=$("q").value.trim();go()},300)};
$("x").onclick=()=>{$("q").value="";q="";go()};
const es=new EventSource("/stream");
es.onmessage=e=>{const n=JSON.parse(e.data),h=norm(n.titulo+" "+(n.resumo||"")+" "+n.fonte);
 const ok=norm(q).split(/\s+/).filter(Boolean).every(w=>h.includes(w));
 if(ok&&(!selT.size||selT.has(n.tema))&&(!selF.size||selF.has(n.fonte))){const v=L.querySelector(".vazio");if(v)v.remove();L.prepend(card(n,true))}
 clearTimeout(timer);timer=setTimeout(filtros,2000)};
(async()=>{try{const r=await (await fetch("/api/filtros")).json();pop=r.populares||[];selT=new Set(pop)}catch(e){}go()})();
setInterval(()=>{if(!aberto)filtros()},30000);
(function(){
const au=new Audio(),$r=id=>document.getElementById(id),ls=(k,v)=>{try{if(v===undefined)return localStorage.getItem(k);localStorage.setItem(k,v)}catch(e){return null}};
let rid=ls("radioId")==="am"?"am":"fm",info=null,hls=null,tocando=false,fallback=false;
au.preload="none";const v0=ls("radioVol");au.volume=v0==null?.7:Math.min(1,Math.max(0,(+v0||0)/100));au.muted=true;   // sempre começa no mudo: navegadores permitem autoplay mudo$r("rvol").value=Math.round(au.volume*100);
const msg=t=>{$r("rmsg").textContent=t||""};
const avisoMudo=()=>msg(tocando&&au.muted?"🔇 Rádio ao vivo no mudo. Clique em 🔇 para ligar o som.":"");
function ui(){$r("rplay").textContent=tocando?"❚❚":"▶";$r("rplay").setAttribute("aria-label",tocando?"Pausar":"Tocar");
 $r("rmute").textContent=(au.muted||au.volume===0)?"🔇":"🔊";$r("radio").classList.toggle("on",tocando);
 document.querySelectorAll(".rband button").forEach(b=>b.classList.toggle("on",b.dataset.r===rid))}
function pausar(){try{au.pause()}catch(e){}tocando=false;ui()}
function embedOff(){fallback=false;$r("radio").classList.remove("fb");$r("rfb").hidden=true;$r("rfb").removeAttribute("src")}
function usarEmbed(){if(fallback)return;fallback=true;pausar();const f=$r("rfb");f.src=(info&&info.embed)||"https://aovivo.ebc.com.br/embed-audio.html?emissora=radio-nacional-fm-de-brasilia";f.hidden=false;
 $r("radio").classList.add("fb");msg("Não foi possível usar o player próprio agora. Use o player oficial da EBC abaixo.")}
function carregaHls(){return new Promise((ok,er)=>{if(window.Hls)return ok();const s=document.createElement("script");s.src="https://cdn.jsdelivr.net/npm/hls.js@1.5.13/dist/hls.min.js";s.onload=ok;s.onerror=er;document.head.appendChild(s)})}
async function fonte(url,tipo){if(hls){hls.destroy();hls=null}
 if(tipo==="hls"&&!au.canPlayType("application/vnd.apple.mpegurl")){try{await carregaHls();if(!window.Hls||!Hls.isSupported())throw 0;hls=new Hls();hls.on(Hls.Events.ERROR,(e,d)=>{if(d&&d.fatal)usarEmbed()});hls.loadSource(url);hls.attachMedia(au);return}catch(e){usarEmbed();return}}
 au.src=url}
async function carregar(id){rid=id;ls("radioId",id);pausar();embedOff();msg("Conectando à rádio…");
 try{info=await (await fetch("/api/radio/"+id)).json()}catch(e){info=null}
 $r("rnome").textContent=info?info.nome:"Rádio Nacional";$r("rsub").textContent=(info?info.freq:"")+" · ao vivo";
 if(!info||!info.stream){usarEmbed();return false}
 await fonte(info.stream,info.tipo);if(!fallback)msg("");return !fallback}
function aoVivo(){if(hls&&hls.liveSyncPosition){try{au.currentTime=hls.liveSyncPosition}catch(e){}}else if(!hls&&au.src){au.load()}}
function tocar(){if(fallback||(!au.src&&!hls))return Promise.resolve();
 return au.play().then(()=>{tocando=true;ui();avisoMudo()}).catch(e=>{tocando=false;ui();if(e&&e.name==="NotAllowedError")throw e;usarEmbed()})}
au.onerror=()=>usarEmbed();au.onplaying=()=>{tocando=true;ui();avisoMudo()};au.onpause=()=>{tocando=false;ui()};au.onwaiting=()=>{if(tocando)msg("Carregando…")};
$r("rplay").onclick=()=>{if(fallback){msg("Use o player oficial da EBC abaixo.");return}
 if(tocando){pausar();ls("radioAuto","0")}else{ls("radioAuto","1");aoVivo();tocar().catch(()=>msg("O navegador bloqueou o áudio. Clique em ▶ de novo."))}};
$r("rmute").onclick=()=>{au.muted=!au.muted;ui();avisoMudo()};
$r("rvol").oninput=e=>{au.volume=e.target.value/100;au.muted=false;ls("radioVol",e.target.value);ui();avisoMudo()};
document.querySelectorAll(".rband button").forEach(b=>b.onclick=()=>{const id=b.dataset.r;if(id===rid&&!fallback)return;const era=tocando||ls("radioAuto")!=="0";
 carregar(id).then(ok=>{if(ok&&era){aoVivo();tocar().catch(()=>{})}})});
async function iniciar(){ui();const ok=await carregar(rid);if(!ok||ls("radioAuto")==="0")return;
 try{await tocar()}catch(e){msg("▶ Toque em qualquer lugar da página para começar a ouvir a Rádio Nacional.");
  const ev=["pointerdown","keydown"],go=x=>{if(tocando||ls("radioAuto")==="0"){ev.forEach(t=>document.removeEventListener(t,go,true));return}
   if(x.target.closest&&x.target.closest("#radio"))return;ev.forEach(t=>document.removeEventListener(t,go,true));aoVivo();tocar().catch(()=>{})};
  ev.forEach(t=>document.addEventListener(t,go,true))}}
iniciar()})();
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
