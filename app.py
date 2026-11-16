"""Agregador de notícias em tempo real. Veja o README.md para instruções."""
import base64, hashlib, json, os, re, socket, sqlite3, threading, time, urllib.request
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
INTERVALO = int(os.environ.get("INTERVALO_SEGUNDOS", "300"))   # checa as fontes a cada 2 min
NOME_SITE = os.environ.get("NOME_SITE", "Pindorama")
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
        for coluna in ("tema", "busca", "credito"):
            try:
                c.execute(f"ALTER TABLE noticias ADD COLUMN {coluna} TEXT")
            except sqlite3.OperationalError:
                pass
        c.execute("CREATE INDEX IF NOT EXISTS i_pub ON noticias(publicado DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS i_fonte ON noticias(fonte)")
        c.execute("CREATE INDEX IF NOT EXISTS i_tema ON noticias(tema)")
        c.execute("CREATE TABLE IF NOT EXISTS logos(nome TEXT PRIMARY KEY, tipo TEXT, dados BLOB)")
        c.execute("CREATE TABLE IF NOT EXISTS acessos(tema TEXT PRIMARY KEY, total INTEGER DEFAULT 0)")
        c.execute("CREATE TABLE IF NOT EXISTS controle(url TEXT PRIMARY KEY, etag TEXT, modified TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS estado(nome TEXT PRIMARY KEY, feed_url TEXT, tentativa INTEGER, ultima INTEGER, status TEXT)")
        for coluna in ("proxima", "espera"):
            try:
                c.execute(f"ALTER TABLE estado ADD COLUMN {coluna} INTEGER")
            except sqlite3.OperationalError:
                pass
        for velho, novo in MAPA_TEMAS.items():
            c.execute("UPDATE noticias SET tema=? WHERE tema=?", (novo, velho))
            c.execute("DELETE FROM acessos WHERE tema=?", (velho,))


def limpar(txt, limite=300):
    txt = unescape(re.sub(r"<[^>]+>", " ", (txt or "")[:6000]))
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt if len(txt) <= limite else txt[:limite].rsplit(" ", 1)[0] + "…"


def link_limpo(url):
    p = urlsplit(url)
    return urlunsplit((p.scheme, p.netloc.lower(), p.path.rstrip("/"), "", ""))


DIAS_RETENCAO = int(os.environ.get("DIAS_RETENCAO", "30"))      # por quantos dias guardar as notícias
MAX_NOTICIAS = int(os.environ.get("MAX_NOTICIAS", "4000"))      # limite total de notícias guardadas
EMAIL_CONTATO = os.environ.get("EMAIL_CONTATO", "")             # e-mail para pedidos de remoção (aparece no rodapé)

_PT = set("não uma são foi com mais das ao pelo pela nas isso também já após tem seu sua ou mas aos às um em da e".split())
_EN = set("the and of to is for with that are this by from has was will new after about his her you they have not but on in".split())
_ES = set("los las del el y es su sus pero más también donde cuando hay fue una con".split())
_FR = set("le les des du une et est pour dans qui sur pas plus avec au aux ce cette sont".split())


def idioma_ok(texto):
    """False quando o texto parece estar em inglês, espanhol ou francês (o portal é em português)."""
    pal = re.findall(r"[a-záéíóúâêôãõçàüñ']+", (texto or "").lower())
    if len(pal) < 5:
        return True
    pt = sum(p in _PT for p in pal)
    fora = max(sum(p in s for p in pal) for s in (_EN, _ES, _FR))
    return not (fora >= 2 and fora > pt)


def https(u):
    return "https://" + u[7:] if u and u.startswith("http://") else u


def achar_imagem(e):
    cand = []
    for chave in ("media_thumbnail", "media_content"):
        for m in e.get(chave, []) or []:
            if m.get("url") and "video" not in str(m.get("type", "")):
                cand.append(m["url"])
    for l in e.get("links", []):
        if l.get("rel") == "enclosure" and str(l.get("type", "")).startswith("image"):
            cand.append(l.get("href"))
    im = e.get("image")
    if isinstance(im, dict):
        cand.append(im.get("href") or im.get("url"))
    blocos = [(e.get("summary") or "")[:4000]] + [(c.get("value") or "")[:8000] for c in e.get("content", []) or []]
    for b in blocos:
        m = re.search(r'<img[^>]+(?:data-src|src)="([^"]+)"', b)
        if m:
            cand.append(unescape(m.group(1)))
    for u in cand:
        if u and u.startswith(("http://", "https://")) and not re.search(r"pixel|spacer|1x1|\.gif(\?|$)|feedburner|/emoji", u, re.I):
            return https(u)
    return None


def achar_credito(e, fonte):
    for chave in ("media_credit", "media_copyright"):
        v = e.get(chave)
        if isinstance(v, list) and v:
            v = v[0]
        if isinstance(v, dict):
            v = v.get("content")
        if isinstance(v, str) and len(v.strip()) > 1:
            return "Foto: " + limpar(v, 70)
    blocos = " ".join([(e.get("summary") or "")[:4000]] + [(c.get("value") or "")[:8000] for c in e.get("content", []) or []])
    m = re.search(r"(?:Foto|Fotografia|Imagem|Cr[eé]dito)s?(?:\s+d[ae]\s+(?:capa|divulga[cç][aã]o))?\s*[:\-–]\s*([^<\n|]{2,70})", blocos, re.I)
    if m and len(m.group(1).strip()) > 1:
        return "Foto: " + limpar(m.group(1), 60)
    return "Imagem: " + fonte


def og_image(url):
    """Quando o feed não traz imagem, procura a imagem de capa na própria matéria."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": AGENTE})
        with urllib.request.urlopen(req, timeout=6) as r:
            html = r.read(80000).decode("utf-8", "ignore")
    except Exception:
        return None
    for pat in (r'<meta[^>]+property=["\']og:image(?::secure_url)?["\'][^>]*content=["\']([^"\']+)',
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image',
                r'<meta[^>]+name=["\']twitter:image["\'][^>]*content=["\']([^"\']+)'):
        m = re.search(pat, html, re.I)
        if m:
            return https(urljoin(url, unescape(m.group(1))))
    return None


_LOGO_TENT = {}


def garantir_logo(f):
    """Guarda o logo (ícone) do portal para usar quando a matéria não tem imagem."""
    nome = f["nome"]
    with db(8) as c:
        if c.execute("SELECT 1 FROM logos WHERE nome=?", (nome,)).fetchone():
            return
    if time.time() - _LOGO_TENT.get(nome, 0) < 86400:
        return
    _LOGO_TENT[nome] = time.time()
    try:
        html = baixar(f["site"], 10)
    except Exception:
        html = ""
    cand = []
    for tag in re.findall(r"<link[^>]+>", html, re.I):
        if re.search(r'rel=["\'][^"\']*icon', tag, re.I):
            h = re.search(r'href=["\']([^"\']+)', tag, re.I)
            if h:
                t = re.search(r'sizes=["\'](\d+)', tag, re.I)
                peso = int(t.group(1)) if t else (512 if "any" in tag.lower() else (180 if "apple" in tag.lower() else 32))
                cand.append((peso, urljoin(f["site"], unescape(h.group(1)))))
    cand = [u for _, u in sorted(cand, reverse=True)] + [urljoin(f["site"], "/favicon.ico")]
    for u in cand[:4]:
        try:
            req = urllib.request.Request(u, headers={"User-Agent": AGENTE})
            with urllib.request.urlopen(req, timeout=8) as r:
                dados, tipo = r.read(200001), r.headers.get_content_type()
            if 100 < len(dados) <= 200000 and (tipo.startswith("image/") or u.endswith(".ico")):
                with ESCRITA, db() as c:
                    c.execute("INSERT OR REPLACE INTO logos(nome,tipo,dados) VALUES(?,?,?)",
                              (nome, tipo if tipo.startswith("image/") else "image/x-icon", dados))
                return
        except Exception:
            continue


def limpar_antigas():
    corte = int(time.time()) - DIAS_RETENCAO * 86400
    with ESCRITA, db() as c:
        c.execute("DELETE FROM noticias WHERE publicado<?", (corte,))
        c.execute("DELETE FROM noticias WHERE id NOT IN (SELECT id FROM noticias ORDER BY publicado DESC LIMIT ?)", (MAX_NOTICIAS,))


import unicodedata

ULTIMA = {"t": None}      # hora da última checagem de fontes

MAPA_TEMAS = {"Sociedade": "Pensamento e Sociedade", "Filosofia": "Pensamento e Sociedade", "Cinema": "Cinema e Entretenimento",
              "Entretenimento": "Cinema e Entretenimento", "Artes": "Artes e Arquitetura", "Ciência": "Ciência e Tecnologia",
              "Tecnologia": "Ciência e Tecnologia"}

TEMAS = {
 "Política": "governo president* lula bolsonaro congresso senado senador* camara deputad* eleic* eleitor* partido* ministr* stf supremo politic* golpe democracia planalto prefeit* governador* oposicao tse impeachment orcamento voto* esquerda direita ditadura sindicat* greve mst pt psol pl",
 "Internacional": "eua estados unidos trump china russia ucrania israel gaza palestin* guerra otan onu europa argentina venezuela cuba franca america latina africa oriente medio biden milei putin internacional imperialismo",
 "Pensamento e Sociedade": "racism* racial genero feminis* lgbt* direitos humanos educacao saude desigualdade periferia violencia justica trabalho previdencia salario sociologia antropolog* filosofia filosof* etica pensador* pensamento metafisica epistemologia fenomenologia kant hegel nietzsche marx* foucault dialetica comportamento familia juventude",
 "Decolonial": "decolonial* descoloniz* colonial* colonialismo colonizacao indigen* quilomb* afro* negro negros negra negras ancestral* ancestralidade epistemolog* originarios racismo",
 "Meio Ambiente": "ambient* clima climatic* aquecimento desmatamento floresta* amazonia queimada* biodiversidade sustentab* ecolog* poluicao carbono emissoes fauna flora oceano mineracao garimpo agrotoxic* cop30 bioma* cerrado pantanal",
 "Identidade Brasileira": "brasileir* patrimonio iphan memoria tradicao tradicoes folclore regional* identidade historia saberes museu da pessoa",
 "Música": "music* musico* cantor* cantora show* album* banda* disco* festival rock samba mpb rap funk jazz forro sertanej* dj single turne clipe compositor* orquestra violao bossa nova hip hop",
 "Cinema e Entretenimento": "filme* cinema* cineast* documentario* curta-metragem longa-metragem oscar cannes roteir* audiovisual atriz ator diretor* mubi serie series novela* celebridade* reality bbb famos* humor game* jogo* streaming netflix podcast tv televisao quadrinho* anime",
 "Artes e Arquitetura": "arte artes artista* exposic* museu* galeria fotograf* escultur* pintur* instalac* bienal curador* teatro danca performance acervo arquitet* urbanismo urbano cidade* design paisagismo edificio",
 "Literatura": "livro* literatur* escritor* escritora romance poesia poema* poet* autor* autora editora ensaio* resenha conto* leitura prosa",
 "Ciência e Tecnologia": "ciencia* cientist* pesquisa* universo astronom* espaco nasa biolog* fisica quimica dinossauro* descobert* planeta cerebro tecnologia tecnologic* tecnolog* digital digitais inteligencia artificial ia algoritm* aplicativo* app software hardware celular smartphone* internet plataforma* startup* privacidade ciberseguranc* hacker* cibernetic* robo* chatgpt openai google apple microsoft tiktok instagram whatsapp bitcoin cripto*",
 "Voluntariado": "voluntari* doacao doacoes doar solidariedade ong ongs mutirao caridade filantrop* comunitari* humanitari*",
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
          "Pensamento e Sociedade": ["direitos humanos"],
          "Decolonial": ["povos originarios"],
          "Meio Ambiente": ["meio ambiente", "mata atlantica", "mudancas climaticas"],
          "Identidade Brasileira": ["cultura popular", "identidade nacional", "historia de vida"],
          "Música": ["bossa nova", "hip hop"],
          "Ciência e Tecnologia": ["inteligencia artificial", "redes sociais", "big tech"]}
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
    pend = []
    with ESCRITA, db() as c:
        for e in d.entries[:20]:
            link, titulo = e.get("link"), limpar(e.get("title"), 250)
            if not link or not titulo:
                continue
            if not idioma_ok(titulo + " " + limpar(e.get("summary"), 400)):
                continue                      # matéria em língua estrangeira
            h = hashlib.sha1(link_limpo(link).encode()).hexdigest()
            ht = hashlib.sha1(re.sub(r"\W+", "", titulo.lower()).encode()).hexdigest()
            if c.execute("SELECT 1 FROM noticias WHERE hash=? OR hash_titulo=?", (h, ht)).fetchone():
                continue
            t = e.get("published_parsed") or e.get("updated_parsed")
            pub = min(timegm(t), agora) if t else agora
            if pub < agora - DIAS_RETENCAO * 86400:
                continue                      # mais antiga que o período de guarda
            img = achar_imagem(e)
            cur = c.execute("""INSERT OR IGNORE INTO noticias(hash,hash_titulo,titulo,resumo,link,imagem,fonte,categoria,publicado,tema,busca,credito)
                         VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (h, ht, titulo, limpar(e.get("summary")), link, img,
                       f["nome"], f.get("categoria", "Geral"), pub,
                       classificar(titulo, limpar(e.get("summary")), f.get("tema")),
                       _norm(titulo + " " + limpar(e.get("summary")) + " " + f["nome"]),
                       achar_credito(e, f["nome"])))
            novas += cur.rowcount
            if cur.rowcount and not img:
                pend.append((cur.lastrowid, link))
        c.execute("INSERT OR REPLACE INTO controle VALUES(?,?,?)",
                  (feed, getattr(d, "etag", None), getattr(d, "modified", None)))
    for rid, lk in pend[:3]:                  # imagem de capa direto da matéria (poucas por rodada)
        im = og_image(lk)
        if im:
            with ESCRITA, db() as c2:
                c2.execute("UPDATE noticias SET imagem=?, credito=? WHERE id=?", (im, "Imagem: " + f["nome"], rid))
    garantir_logo(f)
    estado(f["nome"], status="OK", ultima=agora)
    return novas


def coletar_seguro(f):
    try:
        with db(8) as c:
            est = c.execute("SELECT proxima,espera FROM estado WHERE nome=?", (f["nome"],)).fetchone()
        if est and est["proxima"] and time.time() < est["proxima"]:
            return                          # fonte tranquila: só volta a ser consultada mais tarde
        n = coletar_fonte(f)
        if n:
            print(f"[{f['nome']}] {n} nova(s)")
        espera = INTERVALO if n else min(1800, int(((est and est["espera"]) or INTERVALO) * 1.6))
        estado(f["nome"], espera=espera, proxima=int(time.time()) + espera)
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
        try:
            limpar_antigas()
        except Exception as erro:
            print("Erro ao limpar notícias antigas:", erro)
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


ICONE48 = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAADAAAAAwCAYAAABXAvmHAAABSElEQVR42u2ZwRGEIAxFLYT73vdGISmERiiEQtKDZwrZvegM46AiJIGMHP6RMU9/QhKX9fNdNGuZABNgbADYpBYgbFIL8NukEgASANAIgAkAagMwSfC7jCYAlwFwmgAwA4BaAHL2YbORlH3YbCRlHzYbUQdvL4LfZUcG8AUAfmSAWAAQuQGgUq4g+DSZa59zCWAL32IvxWMOndVxHDB4zN0jrQkpJV+bxNDZUvGuFS9tDXAUy7SUUT+CZVrvARAIHrgvMsOUF7GmW23dOFAFH3q1El7S7xwASFRtugAYQguZHgDQq/LMJD6sDakkCgAPanvktJHU6Mg2anKMjmcdJBScEwEwN8lobs4GynJKtbiKDz189jWcBADW9O2FcwZKAFCvCl1LOa0tn7jSLmrTr8E6DwTqzVqmPAdOAMsYfNUz5o/uCfB2gD9b6wCAxU6iqQAAAABJRU5ErkJggg==")
ICONE180 = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAALQAAAC0CAYAAAA9zQYyAAAFJklEQVR42u3dvXHbQBiEYRWC3LkzFMJC0AgKYSHsgTEKkT0jekb2WBxSxM/dfk+wuXh8dVx8u7h7u/74+UaUIotAgCYCNBGgiQBNgCYCNBGgiQBNBGgCNBGgiQBNBOiOdbrJWgA6QuebrAWgu9fwW+83DdYD0L1r+gT0ZD0A3bsun4C+WA9Ap9gNtgPQUXaD7QB0lN1gOwAdZzfYDkB3q/kO0LP1AXRvWu4AvVgfQPek8Q7MfzRaJ0An2A22A9BRdoPtAHSc3WA7AB1lN9gOQHej9ydlzQDddJH/WaAV/wHddJH/WaAV/wEdYzfYDkBH2Q22A9BRdoPtAHR3zbpHpYEH6KaL/M9K8R/QTRf5n5XiP6Bj7AbbAegou8F2ADrKbrAdgI6zG2wHoLtq1mngATqiyP+sFP8B3XyR/1kp/gM6wm6wHYCOshtsB6Dj7AbbAegou8F2ALqbIr/iP6CjivyK/4COKvIr/gO6rN1gOwAdZTfYDkB31azTwAN0XLNOAw/QEUV+xX9Al7QbbAeg4+wG2wHoKLvBdgA6ym6wHYDurlmngQfoqGadBh6gI4r8iv+ALms32A5AR9kNtgPQUXaD7QB0nN1gO4oAPdxqlt/RfCvS/6ulYaCXL/7m+YV1GCoCPT6wMNMXi/2I3qlLfff7nh7gadx6hwYeNfsKWs/dBsrWtLeHHhv3mdSnllceetd4GGNBaE2LMVwbmHKwINREPXbtuS0LQrtajK3n0EMj3WHqQ5e159+VOhBUoJOy9aEsLAj9z2JsdnjOHpE0C0KbWYyjuhwsCO1Se00/F47a0G7n8x3RiuOra/nlXVt8Fc5Ypk6KRb33oVkQFiOu4M+CsBiRb6ywICxG3CtYCk7Fi0WJ7xQqOBUuFqW+JKtjXaS7XO2tbxaExYg7xoAFYTHizuVQcCpSLKoCtIKT8/RiT07SsQ7qLgOaBWExgs+2Y0FYjLjDGhWc3C0ed/qoglNIsQjQCk5RxSJAsyDlLUY60CxIMYtRAWgWpJDFqAS0glNnxSJAKzhFFYsALV2MTv0AfV/Vd2jXuoXZjupAj4B2xbF+BqBdcdzwQyGgQx4Ijes+NADaLNoMGtBNycsAf4/vAG1cZ3wHaO077TpAKygpJgGa3WA7SgAtHSyYGkoHpYaAlg5KDQEtHZQaAlo6KDUEtHSwcGpoXGd8B2jpoNQQ0NJBqSGg2Q22ozbQ0sHiqaF0cL8go9WgZwa0dPA79/m1ev/iAmjp4Cv3k7R4T8wAaOngK7estrZbT4CWDq4BRyv/hBdAG9etdX5cK+fuAVo6uOpP9tG79QnQ0sG1j6g98ujfM6Br240t57cz21EX6LHzXbml3XoEdK108Hzdd2a793hvBnSNdPDoS9xPO35OQIeng3vvykfv1gOgc9PByWcGdEI62PolO8PGnx3QQeO6yS8UoBPSwV7v8dtivHcCdN/pYELJfb5KDd+q242021XX3K0B3Vk6eL5mHoe11nhvBHQfP6/LNfwU+5XCmBnQ7aeDqbvyFrv1Aui208GpEMhrjfcGQLf35VyK7cprhjEToNtKBycgv7QhXADdxrgubRx35HgP0AengzNgV50WnQB9TDpoV95mtz4Den+7cfbgt+l4D9A7pYNVQpKjw5gR0Nv7Pbvyfrv1DOhtx3XGcfuO9y6A3iYdFJIcF8YMgF53t7ArW/+oHRpYvoMyR4ERAZoATQRoIkATAZoAbREI0ESAJgI0EaAJ0ESAJgI0EaCJAE319Avmf9xzcczOUgAAAABJRU5ErkJggg==")
ICONE_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><polygon fill="#D92732" points="32.0,5.0 38.8,24.6 59.6,25.0 43.0,37.6 49.0,57.5 32.0,45.6 15.0,57.5 21.0,37.6 4.4,25.0 25.2,24.6"/></svg>'


@app.route("/favicon.svg")
def favicon_svg():
    return Response(ICONE_SVG, mimetype="image/svg+xml", headers={"Cache-Control": "public, max-age=604800"})


@app.route("/favicon.ico")
def favicon_ico():
    return Response(ICONE48, mimetype="image/png", headers={"Cache-Control": "public, max-age=604800"})


@app.route("/icone.png")
def icone_png():
    return Response(ICONE180, mimetype="image/png", headers={"Cache-Control": "public, max-age=604800"})


@app.route("/logo/<path:nome>")
def logo(nome):
    with db(8) as c:
        r = c.execute("SELECT tipo,dados FROM logos WHERE nome=?", (nome,)).fetchone()
    if not r:
        return "", 404
    return Response(r["dados"], mimetype=r["tipo"], headers={"Cache-Control": "public, max-age=86400"})


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
        logos = [r[0] for r in c.execute("SELECT nome FROM logos")]
        pop = [r["tema"] for r in c.execute("SELECT tema FROM acessos WHERE total>0 ORDER BY total DESC LIMIT 3")]
        for r in c.execute("SELECT tema FROM noticias WHERE tema IS NOT NULL GROUP BY tema ORDER BY COUNT(*) DESC"):
            if len(pop) >= 3:
                break
            if r["tema"] not in pop:
                pop.append(r["tema"])
    return jsonify(temas=temas, fontes=fontes, atualizado=ULTIMA["t"] or ult, populares=pop, logos=logos)


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
<link rel="icon" type="image/svg+xml" href="/favicon.svg"><link rel="icon" type="image/png" sizes="48x48" href="/favicon.ico"><link rel="apple-touch-icon" href="/icone.png"><meta name="theme-color" content="#D92732">
<style>
:root{--blue:#1557A6;--yel:#F2C230;--bg:#f7f4ec;--tx:#171717;--mut:#5f5a52;--red:#D92732;--glass:rgba(255,255,255,.44);--edge:rgba(255,255,255,.5);--sh:rgba(160,30,55,.24);--pnl:rgba(255,255,255,.86);--b1:.5;--b2:.32;--b3:.42;--b4:.28}
@media(prefers-color-scheme:dark){:root{--bg:#171717;--tx:#f6f3ec;--mut:#aaa59b;--red:#F0525D;--glass:rgba(255,255,255,.07);--edge:rgba(255,255,255,.11);--sh:rgba(0,0,0,.55);--pnl:rgba(30,30,30,.9);--b1:.28;--b2:.22;--b3:.2;--b4:.16}}
*{box-sizing:border-box}html{background:var(--bg)}
body{margin:0;color:var(--tx);font:16px/1.4 "SF Pro Display","Inter","Segoe UI",system-ui,"Helvetica Neue",Arial,sans-serif;min-height:100vh}
body::before{content:"";position:fixed;inset:-12%;z-index:-2;filter:blur(40px);background:
radial-gradient(36% 32% at 8% 8%,rgba(242,194,48,calc(var(--b1) * .9)),transparent 70%),
radial-gradient(34% 38% at 92% 14%,rgba(217,39,50,var(--b2)),transparent 70%),
radial-gradient(42% 40% at 76% 90%,rgba(21,87,166,var(--b3)),transparent 70%),
radial-gradient(38% 38% at 6% 84%,rgba(21,87,166,var(--b4)),transparent 70%)}
body::after{content:"";position:fixed;inset:0;z-index:-1;pointer-events:none;opacity:.07;background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='180' height='180'><filter id='n'><feTurbulence type='fractalNoise' baseFrequency='.9' numOctaves='2'/></filter><rect width='100%' height='100%' filter='url(%23n)'/></svg>")}

.art{position:absolute;inset:0;width:100%;height:100%;z-index:0;color:var(--tx)}
.hw{position:relative;z-index:1;max-width:1360px;margin:auto;padding:16px 28px 14px;display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:10px 28px}
.kick{display:flex;flex-wrap:wrap;gap:2px 14px;font-size:.68rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--mut)}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--red);margin-right:6px;animation:p 1.6s infinite}@keyframes p{50%{opacity:.25}}
h1{margin:4px 0 0;font-weight:800;font-size:clamp(1.5rem,3.2vw,2.4rem);line-height:1;letter-spacing:-.035em}h1 .st{color:var(--red)}
.search{display:flex;width:min(470px,100%);border-radius:99px;background:var(--glass);border:1px solid var(--edge);backdrop-filter:blur(18px) saturate(1.5);-webkit-backdrop-filter:blur(18px) saturate(1.5);box-shadow:0 10px 30px -12px var(--sh),inset 0 1px 0 rgba(255,255,255,.4);padding:4px}
.search input{flex:1;min-width:0;border:0;background:none;color:inherit;font:inherit;font-size:.98rem;padding:9px 16px;outline:none}
.search button{border:0;border-radius:99px;background:radial-gradient(ellipse at center,rgba(21,87,166,.95) 0%,rgba(21,87,166,.4) 100%);color:#fff;font-weight:700;padding:0 18px;cursor:pointer;font-size:.74rem;letter-spacing:.08em;text-transform:uppercase}
.layout{max-width:1360px;margin:auto;display:flex;gap:30px;padding:14px 28px 60px;align-items:flex-start}
aside{width:250px;flex:none;position:sticky;top:14px;padding:16px;border-radius:24px;background:var(--glass);border:1px solid var(--edge);backdrop-filter:blur(20px) saturate(1.5);-webkit-backdrop-filter:blur(20px) saturate(1.5);box-shadow:0 14px 40px -16px var(--sh),inset 0 1px 0 rgba(255,255,255,.4)}
aside h3{margin:0 0 7px;font-size:.66rem;letter-spacing:.14em;text-transform:uppercase;color:var(--mut)}
.dd{position:relative;margin-bottom:18px}.dd:last-child{margin-bottom:0}
.ddb{width:100%;display:flex;align-items:center;gap:8px;text-align:left;border:1px solid var(--edge);border-radius:16px;background:var(--glass);color:var(--tx);padding:10px 12px;cursor:pointer;font:inherit;font-size:.86rem;box-shadow:inset 0 1px 0 rgba(255,255,255,.35)}
.ddb .l{flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.ddb .c{opacity:.6;transition:transform .2s}.dd.open .c{transform:rotate(180deg)}
.ddp{display:none;position:absolute;left:0;right:0;top:calc(100% + 8px);z-index:30;padding:8px;border-radius:20px;background:var(--pnl);border:1px solid var(--edge);backdrop-filter:blur(26px) saturate(1.6);-webkit-backdrop-filter:blur(26px) saturate(1.6);box-shadow:0 24px 60px -12px var(--sh)}
.dd.open .ddp{display:block}
.acts{display:flex;gap:6px;padding:2px 2px 8px}.acts button{flex:1;border:0;border-radius:99px;padding:6px 8px;font-size:.7rem;font-weight:700;cursor:pointer;color:var(--tx);background:radial-gradient(ellipse at center,rgba(21,87,166,.22),rgba(21,87,166,.06))}
.lst{max-height:300px;overflow-y:auto}
.opt{display:flex;align-items:center;gap:9px;padding:7px 8px;border-radius:12px;cursor:pointer;font-size:.86rem;user-select:none}.opt:hover{background:rgba(217,39,50,.1)}
.opt .k{width:17px;height:17px;flex:none;border-radius:6px;border:1.5px solid var(--mut);display:grid;place-items:center;font-size:.65rem;color:transparent}
.opt.on .k{border-color:transparent;color:#fff;background:radial-gradient(circle,var(--blue) 35%,rgba(21,87,166,.55) 100%)}
.opt .t{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.opt .n{font-size:.72rem;opacity:.6;font-variant-numeric:tabular-nums}
main{flex:1;min-width:0}
.bar{display:flex;justify-content:space-between;align-items:baseline;gap:12px;margin:2px 4px 18px}
.bar h2{margin:0;font-size:1.1rem;font-weight:800;letter-spacing:-.02em}.bar span{font-size:.7rem;letter-spacing:.1em;text-transform:uppercase;color:var(--mut);white-space:nowrap}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(262px,1fr));gap:30px}
a.card{position:relative;display:flex;flex-direction:column;gap:12px;padding:10px 10px 16px;border-radius:26px;overflow:hidden;text-decoration:none;color:inherit;background:var(--glass);border:1px solid var(--edge);backdrop-filter:blur(20px) saturate(1.5);-webkit-backdrop-filter:blur(20px) saturate(1.5);box-shadow:0 16px 44px -18px var(--sh),inset 0 1px 0 rgba(255,255,255,.45);transition:transform .3s,box-shadow .3s}
a.card::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(110% 70% at 0% 0%,var(--c0) 0%,transparent 62%);opacity:.55}
a.card>*{position:relative}a.card:hover{transform:translateY(-4px);box-shadow:0 26px 60px -16px var(--sh),inset 0 1px 0 rgba(255,255,255,.55)}
.im{position:relative;aspect-ratio:16/10;border-radius:18px;overflow:hidden}.im img{width:100%;height:100%;object-fit:cover;display:block;transition:transform .5s}a.card:hover .im img{transform:scale(1.05)}
.ph{width:100%;height:100%;display:grid;place-items:center;font-size:2.8rem;background:radial-gradient(circle at 50% 50%,var(--c0) 0%,var(--c1) 100%)}
.cb{display:flex;flex-direction:column;gap:8px;padding:0 6px;flex:1}
.row{display:flex;justify-content:space-between;align-items:center;gap:10px}
.tag{min-width:0;max-width:64%;overflow:hidden;text-overflow:ellipsis;display:inline-flex;align-items:center;gap:5px;padding:4px 11px;border-radius:99px;font-size:.68rem;font-weight:700;letter-spacing:.04em;white-space:nowrap;background:radial-gradient(ellipse at 50% 50%,var(--c0) 0%,var(--c1) 100%)}
.src{font-size:.7rem;font-weight:600;color:var(--mut);text-align:right;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
a.card h3{margin:0;font-size:1.1rem;line-height:1.15;letter-spacing:-.02em;font-weight:750}
a.card p{margin:0;font-size:.85rem;line-height:1.38;color:var(--mut);display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
a.card time{margin-top:auto;padding-top:6px;font-size:.68rem;letter-spacing:.08em;text-transform:uppercase;color:var(--mut)}
.novo{animation:f 1.8s}@keyframes f{from{box-shadow:0 0 0 3px rgba(217,39,50,.55)}}
.vazio{padding:30px 6px;color:var(--mut)}
#mais{display:block;margin:34px auto 0;padding:12px 32px;border:1px solid var(--edge);border-radius:99px;background:var(--glass);backdrop-filter:blur(16px);color:var(--tx);font-weight:700;letter-spacing:.08em;text-transform:uppercase;font-size:.74rem;cursor:pointer}
#mais:hover{background:radial-gradient(ellipse at center,rgba(21,87,166,.9),rgba(21,87,166,.45));color:#fff}
#fb{display:none;margin:6px 28px 0;padding:9px 18px;border:1px solid var(--edge);border-radius:99px;background:var(--glass);color:var(--tx);font-weight:700;font-size:.78rem}
@media(max-width:860px){.layout{flex-direction:column;padding:12px 16px 50px;gap:18px}aside{display:none;width:100%;position:static}aside.open{display:block}#fb{display:inline-block;margin-left:16px}.hw{padding:14px 16px 10px}.grid{gap:20px;grid-template-columns:repeat(auto-fill,minmax(220px,1fr))}}
.orbs{position:fixed;inset:0;z-index:-1;overflow:hidden;pointer-events:none}
.orbs i{position:absolute;left:0;top:0;width:calc(46vmax - var(--k) * 1.8vmax);aspect-ratio:1;border-radius:50%;opacity:calc(.92 - var(--k) * .1);background:radial-gradient(circle at 36% 32%,rgba(242,194,48,.85) 0%,rgba(217,39,50,.6) 36%,rgba(21,87,166,.25) 66%,rgba(21,87,166,0) 100%);animation:orb 64s ease-in-out infinite;animation-delay:calc((var(--k) - 7) * .9s);will-change:transform}
@keyframes orb{0%,100%{transform:translate(calc(-50% + 8vw),calc(-50% + 20vh))}25%{transform:translate(calc(-50% + 64vw),calc(-50% + 6vh))}50%{transform:translate(calc(-50% + 92vw),calc(-50% + 58vh))}75%{transform:translate(calc(-50% + 36vw),calc(-50% + 94vh))}}
@media(prefers-color-scheme:dark){.orbs{opacity:.6}}
@media(prefers-reduced-motion:reduce){.orbs i{animation:none;transform:translate(calc(-50% + 70vw),calc(-50% + 30vh))}}
.im.lg{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:8px}.im.pos{background:linear-gradient(135deg,#fff,#eeebe3)}.im.neg{background:linear-gradient(135deg,#171717,#2c2c2c)}
.im.lg img{max-width:44%;max-height:48%;object-fit:contain;transition:none}a.card:hover .im.lg img{transform:none}
.ln{font-size:.68rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:#171717;text-align:center;padding:0 10px}.neg .ln{color:#fff}
.cr{position:absolute;left:0;right:0;bottom:0;padding:16px 9px 5px;font-size:.62rem;line-height:1.2;color:#fff;background:linear-gradient(transparent,rgba(0,0,0,.62));white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-shadow:0 1px 2px rgba(0,0,0,.6)}
.rodape{max-width:1360px;margin:34px auto 0;padding:0 28px 10px;font-size:.74rem;line-height:1.5;color:var(--mut)}.rodape a{color:var(--blue)}
.manif{margin-top:20px;padding-top:16px;border-top:1px solid var(--edge);font-size:.76rem;line-height:1.5;color:var(--mut)}
.manif .mbar{display:block;width:30px;height:4px;border-radius:4px;background:var(--yel);margin-bottom:9px}
.manif .mt{display:block;margin-bottom:8px;font-size:.86rem;letter-spacing:-.01em;color:var(--tx)}
.manif p{margin:0 0 9px}.manif .ass{margin:12px 0 0;font-weight:800;font-size:.86rem;color:var(--red)}
.manif.mob{display:none;max-width:1360px;margin:30px auto 0;padding:16px 28px 0}
@media(max-width:860px){.manif.mob{display:block}aside .manif{display:none}}
@media(max-height:640px){aside{position:static}}
:root{--hdr:rgba(247,244,236,.76)}@media(prefers-color-scheme:dark){:root{--hdr:rgba(23,23,23,.76)}}
.hero{position:sticky;top:0;z-index:60;background:var(--hdr);border-bottom:1px solid var(--edge);box-shadow:0 14px 34px -24px var(--sh)}
.hero::before{content:"";position:absolute;inset:0;z-index:0;backdrop-filter:blur(22px) saturate(1.6);-webkit-backdrop-filter:blur(22px) saturate(1.6)}
.hw{display:grid;grid-template-columns:auto minmax(0,1fr) minmax(250px,370px);grid-template-areas:"brand radio search";align-items:center;gap:10px 22px;padding:12px 28px}
.hw>.brand{grid-area:brand}.hw>.radio{grid-area:radio;justify-self:center;width:100%;max-width:540px}.hw>.search{grid-area:search;width:100%}
aside{top:calc(var(--hh,96px) + 14px)}
.radio{position:relative;display:flex;align-items:center;gap:10px;padding:6px 8px 6px 6px;border-radius:99px;background:var(--glass);border:1px solid var(--edge);box-shadow:0 10px 30px -14px var(--sh),inset 0 1px 0 rgba(255,255,255,.45);backdrop-filter:blur(16px) saturate(1.5);-webkit-backdrop-filter:blur(16px) saturate(1.5);min-width:0}
#rplay{width:42px;height:42px;flex:none;border-radius:50%;border:0;color:#fff;font-size:.95rem;cursor:pointer;background:radial-gradient(circle at 35% 30%,#ff7a8c,rgba(217,39,50,.95) 60%,rgba(140,20,30,.72));box-shadow:0 8px 20px -6px rgba(217,39,50,.7)}
.rinfo{display:flex;align-items:center;gap:9px;flex:1 1 auto;min-width:0}
.eq{display:flex;align-items:flex-end;gap:2px;height:16px;flex:none}.eq i{width:3px;height:5px;border-radius:2px;background:var(--red)}
.radio.on .eq i{animation:eq 1s ease-in-out infinite}.radio.on .eq i:nth-child(2){animation-delay:-.4s}.radio.on .eq i:nth-child(3){animation-delay:-.75s}
@keyframes eq{0%,100%{height:4px}50%{height:16px}}
.rtxt{min-width:0}.rtxt b{display:block;font-size:.84rem;line-height:1.15;letter-spacing:-.01em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.rtxt small{display:block;font-size:.64rem;letter-spacing:.07em;text-transform:uppercase;color:var(--mut);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.rtxt small.alert{color:var(--red);font-weight:700}
.rband{display:flex;gap:2px;padding:2px;border-radius:99px;background:rgba(21,87,166,.1);flex:none}
.rband button{border:0;border-radius:99px;padding:5px 10px;font-size:.66rem;font-weight:700;background:none;color:var(--tx);cursor:pointer;white-space:nowrap}
.rband button.on{background:radial-gradient(ellipse at center,rgba(21,87,166,.95),rgba(21,87,166,.5));color:#fff}
.rvolw{display:flex;align-items:center;gap:4px;flex:none}
#rmute{border:0;background:none;font-size:1.1rem;cursor:pointer;padding:4px;line-height:1;color:var(--tx)}
#rvol{width:80px;accent-color:var(--red)}
#rmin{width:28px;height:28px;flex:none;border-radius:50%;border:1px solid var(--edge);background:none;color:var(--tx);cursor:pointer;font-size:1rem;line-height:1}
.radio button:focus-visible,.radio input:focus-visible{outline:2px solid var(--blue);outline-offset:2px}
.radio.min .rband,.radio.min .rvolw{display:none}.radio.min{max-width:300px}
.rfbwrap{display:none;position:absolute;top:calc(100% + 10px);right:0;width:min(420px,100%);padding:10px;border-radius:20px;background:var(--pnl);border:1px solid var(--edge);box-shadow:0 20px 50px -14px var(--sh);z-index:5}
.radio.fb .rfbwrap{display:block}
#rfb{display:block;width:100%;height:96px;border:0;border-radius:14px}#rfb[hidden]{display:none}
#rlink{display:block;margin-top:8px;font-size:.74rem;color:var(--blue)}
@media(prefers-reduced-motion:reduce){.radio.on .eq i{animation:none;height:10px}}
@media(max-width:1199px){#rvol{display:none}}
@media(max-width:959px){.hw{grid-template-columns:minmax(0,1fr) minmax(220px,340px);grid-template-areas:"brand search" "radio radio"}.hw>.radio{max-width:none;justify-self:stretch}#rvol{display:block}}
@media(max-width:767px){.hw{grid-template-columns:1fr;grid-template-areas:"brand" "search";padding:10px 16px}h1{font-size:1.45rem}.kick{font-size:.62rem}
 body{padding-bottom:86px}#rvol{display:none}.rband button{padding:5px 8px}
 .hw>.radio{position:fixed;left:50%;bottom:calc(12px + env(safe-area-inset-bottom,0px));transform:translateX(-50%);width:min(560px,calc(100% - 20px));max-width:none;z-index:70;background:var(--pnl);justify-self:auto}
 .radio.min{left:auto;right:10px;transform:none;width:auto}.radio.min .rtxt{display:none}
 .rfbwrap{top:auto;bottom:calc(100% + 10px)}}
</style></head><body>
<div class="orbs" aria-hidden="true"><i style="--k:0"></i><i style="--k:1"></i><i style="--k:2"></i><i style="--k:3"></i><i style="--k:4"></i><i style="--k:5"></i><i style="--k:6"></i><i style="--k:7"></i></div>
<header class="hero" id="hdr">
<svg class="art" viewBox="0 0 1200 140" preserveAspectRatio="xMaxYMid slice" aria-hidden="true">
<defs>
<radialGradient id="sp" cx=".34" cy=".3" r=".9"><stop offset="0" stop-color="#ffc2cf"/><stop offset=".45" stop-color="#f2506f" stop-opacity=".9"/><stop offset="1" stop-color="#b3121f" stop-opacity=".22"/></radialGradient>
<filter id="gr" x="-15%" y="-15%" width="130%" height="130%"><feGaussianBlur in="SourceGraphic" stdDeviation="2" result="b"/>
<feTurbulence type="fractalNoise" baseFrequency=".9" numOctaves="3" seed="7" result="n"/>
<feColorMatrix in="n" type="matrix" values="0 0 0 0 .1  0 0 0 0 0  0 0 0 0 0  0 0 0 1.5 -.62" result="d"/>
<feComposite in="d" in2="b" operator="in" result="dd"/><feMerge><feMergeNode in="b"/><feMergeNode in="dd"/></feMerge></filter>
<filter id="air" x="-60%" y="-60%" width="220%" height="220%"><feGaussianBlur stdDeviation="12"/></filter>
</defs>
<path d="M1200 140 L1200 20 A130 130 0 0 0 1070 140 Z" fill="#D92732" opacity=".45" filter="url(#air)"/>
<circle cx="1170" cy="128" r="46" fill="currentColor" opacity=".5" filter="url(#air)"/>
<circle cx="880" cy="22" r="30" fill="#ff7a59" opacity=".7" filter="url(#air)"/>
</svg>
<div class="hw"><div class="brand"><div class="kick"><span><span class="dot"></span>Ao vivo</span><span>Última atualização: <span id="upd">aguardando…</span></span></div>
<h1><span class="st">★</span> __NOME__</h1></div>
<div id="radio" class="radio" role="region" aria-label="Rádio Nacional ao vivo">
<button type="button" id="rplay" aria-label="Tocar">▶</button>
<div class="rinfo"><span class="eq" aria-hidden="true"><i></i><i></i><i></i></span><div class="rtxt"><b id="rnome">Rádio Nacional FM Brasília</b><small id="rsub">96,1 MHz · ao vivo</small></div></div>
<div class="rband" role="group" aria-label="Emissora"><button type="button" data-r="fm" class="on">FM 96,1</button><button type="button" data-r="am">AM 980</button></div>
<div class="rvolw"><button type="button" id="rmute" aria-label="Mudo">🔊</button><input id="rvol" type="range" min="0" max="100" value="70" aria-label="Volume"></div>
<button type="button" id="rmin" aria-label="Minimizar player" aria-expanded="true" title="Minimizar">–</button>
<div class="rfbwrap"><iframe id="rfb" hidden allow="autoplay" title="Rádio Nacional (player oficial da EBC)"></iframe><a id="rlink" href="https://radionacional.ebc.com.br" target="_blank" rel="noopener">Abrir o site da Rádio Nacional ↗</a></div>
</div>
<form class="search" onsubmit="return false"><input id="q" type="search" placeholder="Pesquisar notícias, temas ou portais…" autocomplete="off" aria-label="Pesquisar"><button type="button" id="x">Limpar</button></form></div>
</header>
<button id="fb">☰ Filtros</button>
<div class="layout"><aside id="side"><h3>Eixos temáticos</h3><div class="dd" id="ddt"></div><h3>Portais</h3><div class="dd" id="ddf"></div><section class="manif"><i class="mbar"></i><b class="mt">Manifesto</b><p>Pindorama. Um agregador de cultura, ideias e informação que conecta diferentes nichos de interesse: política, sociedade, música, cinema, artes, literatura, ciência e tudo aquilo que ajuda a compreender o nosso tempo.</p><p>A proposta é ampliar o acesso a vozes, perspectivas e produções relevantes, com atenção especial ao pensamento brasileiro e à cultura nacional, sem perder de vista o que acontece no mundo. Um espaço plural, crítico e independente, que valoriza a diversidade de ideias, questiona o senso comum e aposta na vanguarda do pensar.</p><p class="ass">Pindorama.</p></section></aside>
<main><div class="bar"><h2 id="titulo">Todas as notícias</h2><span id="cont"></span></div><div class="grid" id="lista"></div><button id="mais">Carregar mais</button></main></div>
<section class="manif mob"><i class="mbar"></i><b class="mt">Manifesto</b><p>Pindorama. Um agregador de cultura, ideias e informação que conecta diferentes nichos de interesse: política, sociedade, música, cinema, artes, literatura, ciência e tudo aquilo que ajuda a compreender o nosso tempo.</p><p>A proposta é ampliar o acesso a vozes, perspectivas e produções relevantes, com atenção especial ao pensamento brasileiro e à cultura nacional, sem perder de vista o que acontece no mundo. Um espaço plural, crítico e independente, que valoriza a diversidade de ideias, questiona o senso comum e aposta na vanguarda do pensar.</p><p class="ass">Pindorama.</p></section>
<footer class="rodape">__NOME__ é um agregador de notícias: exibe apenas título, breve resumo e imagem de capa com crédito, sempre com link para a matéria original. Todo o conteúdo pertence aos respectivos autores e veículos.__CONTATO__</footer>
<script>
const CORES={"Política":"#D92732","Internacional":"#1557A6","Pensamento e Sociedade":"#F2C230","Decolonial":"#9C3D1B","Meio Ambiente":"#3F8F4F","Identidade Brasileira":"#2A86B8","Música":"#E8782E","Cinema e Entretenimento":"#8A3FA0","Artes e Arquitetura":"#171717","Literatura":"#8B4A2B","Ciência e Tecnologia":"#12A0A0","Voluntariado":"#E5646B"};
const ICO={"Política":"🏛️","Internacional":"🌍","Pensamento e Sociedade":"💭","Decolonial":"✊","Meio Ambiente":"🌿","Identidade Brasileira":"🌴","Música":"🎵","Cinema e Entretenimento":"🎬","Artes e Arquitetura":"🎨","Literatura":"📚","Ciência e Tecnologia":"🔬","Voluntariado":"🤝"};
let logos=new Set(),selT=new Set(),selF=new Set(),pop=[],q="",ultimo=null,timer=null,bt=null,aberto=null,dados=null;
const $=id=>document.getElementById(id),L=$("lista");
const norm=t=>(t||"").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"");
const rgba=(h,a)=>{const n=parseInt(h.slice(1),16);return"rgba("+(n>>16)+","+((n>>8)&255)+","+(n&255)+","+a+")"};
function ago(t){const s=Math.max(0,Date.now()/1000-t);if(s<60)return"agora";if(s<3600)return"há "+Math.floor(s/60)+" min";
 if(s<86400)return"há "+Math.floor(s/3600)+" h";return new Date(t*1000).toLocaleDateString("pt-BR")}
function fmt(t){return new Date(t*1000).toLocaleString("pt-BR",{timeZone:"America/Sao_Paulo",day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"}).replace(", "," às ")}
const LUM=new Map();
function lumLogo(img){try{const c=document.createElement("canvas");c.width=c.height=24;const x=c.getContext("2d",{willReadFrequently:true});x.drawImage(img,0,0,24,24);
 const d=x.getImageData(0,0,24,24).data;let s=0,n=0;for(let i=0;i<d.length;i+=4){if(d[i+3]>40){s+=(.2126*d[i]+.7152*d[i+1]+.0722*d[i+2])/255;n++}}return n?s/n:0}catch(e){return 0}}
function card(n,novo){const a=document.createElement("a");a.className="card"+(novo?" novo":"");a.href=n.link;a.target="_blank";a.rel="noopener";
 const c=CORES[n.tema]||"#888";a.style.setProperty("--c0",rgba(c,.5));a.style.setProperty("--c1",rgba(c,.07));
 a.onclick=()=>{try{navigator.sendBeacon("/api/clique",new Blob([JSON.stringify({tema:n.tema})],{type:"application/json"}))}catch(e){}};
 const im=document.createElement("div");im.className="im";
 const logo=()=>{im.innerHTML="";im.className="im lg pos";
  if(logos.has(n.fonte)){const i=new Image();i.alt=n.fonte;i.src="/logo/"+encodeURIComponent(n.fonte);
   i.onload=()=>{let l=LUM.get(n.fonte);if(l==null){l=lumLogo(i);LUM.set(n.fonte,l)}im.classList.remove("pos","neg");im.classList.add(l>.62?"neg":"pos")};
   i.onerror=()=>{i.remove()};im.appendChild(i)}else{const p=document.createElement("div");p.className="ph";p.textContent=ICO[n.tema]||"📰";im.appendChild(p)}
  const s=document.createElement("span");s.className="ln";s.textContent=n.fonte;if(logos.has(n.fonte))im.appendChild(s)};
 if(n.imagem){const i=document.createElement("img");i.src=n.imagem;i.loading="lazy";i.alt="";i.referrerPolicy="no-referrer";i.onerror=logo;im.appendChild(i);
  if(n.credito){const k=document.createElement("span");k.className="cr";k.textContent=n.credito;k.title=n.credito;im.appendChild(k)}}else logo();
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
async function filtros(){dados=await (await fetch("/api/filtros?"+qs())).json();logos=new Set(dados.logos||[]);dd2();
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
(async()=>{try{const r=await (await fetch("/api/filtros")).json();pop=r.populares||[];selT=new Set(pop);logos=new Set(r.logos||[])}catch(e){}go()})();
setInterval(()=>{if(!aberto)filtros()},30000);
const hdr=document.getElementById("hdr"),setHH=()=>document.documentElement.style.setProperty("--hh",hdr.offsetHeight+"px");
setHH();if(window.ResizeObserver)new ResizeObserver(setHH).observe(hdr);else addEventListener("resize",setHH);
(function(){
const au=new Audio(),$r=id=>document.getElementById(id),ls=(k,v)=>{try{if(v===undefined)return localStorage.getItem(k);localStorage.setItem(k,v)}catch(e){return null}};
let rid=ls("radioId")==="am"?"am":"fm",info=null,hls=null,tocando=false,fallback=false;
au.preload="none";const v0=ls("radioVol");au.volume=v0==null?.7:Math.min(1,Math.max(0,(+v0||0)/100));au.muted=true;   // sempre começa no mudo: navegadores permitem autoplay mudo$r("rvol").value=Math.round(au.volume*100);
let subPadrao="96,1 MHz · ao vivo";const msg=t=>{const s=$r("rsub");s.textContent=t||subPadrao;s.title=t||"";s.classList.toggle("alert",!!t)};
const avisoMudo=()=>msg(tocando&&au.muted?"🔇 No mudo · toque no ícone para ouvir":"");
function ui(){$r("rplay").textContent=tocando?"❚❚":"▶";$r("rplay").setAttribute("aria-label",tocando?"Pausar":"Tocar");
 $r("rmute").textContent=(au.muted||au.volume===0)?"🔇":"🔊";$r("radio").classList.toggle("on",tocando);
 document.querySelectorAll(".rband button").forEach(b=>b.classList.toggle("on",b.dataset.r===rid))}
function pausar(){try{au.pause()}catch(e){}tocando=false;ui()}
function embedOff(){fallback=false;$r("radio").classList.remove("fb");$r("rfb").hidden=true;$r("rfb").removeAttribute("src")}
function usarEmbed(){if(fallback)return;fallback=true;pausar();const f=$r("rfb");f.src=(info&&info.embed)||"https://aovivo.ebc.com.br/embed-audio.html?emissora=radio-nacional-fm-de-brasilia";f.hidden=false;
 $r("radio").classList.add("fb");msg("Usando o player oficial da EBC")}
function carregaHls(){return new Promise((ok,er)=>{if(window.Hls)return ok();const s=document.createElement("script");s.src="https://cdn.jsdelivr.net/npm/hls.js@1.5.13/dist/hls.min.js";s.onload=ok;s.onerror=er;document.head.appendChild(s)})}
async function fonte(url,tipo){if(hls){hls.destroy();hls=null}
 if(tipo==="hls"&&!au.canPlayType("application/vnd.apple.mpegurl")){try{await carregaHls();if(!window.Hls||!Hls.isSupported())throw 0;hls=new Hls();hls.on(Hls.Events.ERROR,(e,d)=>{if(d&&d.fatal)usarEmbed()});hls.loadSource(url);hls.attachMedia(au);return}catch(e){usarEmbed();return}}
 au.src=url}
async function carregar(id){rid=id;ls("radioId",id);pausar();embedOff();msg("Conectando…");
 try{info=await (await fetch("/api/radio/"+id)).json()}catch(e){info=null}
 $r("rnome").textContent=info?info.nome:"Rádio Nacional";subPadrao=(info?info.freq:"")+" · ao vivo";if(!$r("rsub").classList.contains("alert"))$r("rsub").textContent=subPadrao;
 if(!info||!info.stream){usarEmbed();return false}
 await fonte(info.stream,info.tipo);if(!fallback)msg("");return !fallback}
function aoVivo(){if(hls&&hls.liveSyncPosition){try{au.currentTime=hls.liveSyncPosition}catch(e){}}else if(!hls&&au.src){au.load()}}
function tocar(){if(fallback||(!au.src&&!hls))return Promise.resolve();
 return au.play().then(()=>{tocando=true;ui();avisoMudo()}).catch(e=>{tocando=false;ui();if(e&&e.name==="NotAllowedError")throw e;usarEmbed()})}
au.onerror=()=>usarEmbed();au.onplaying=()=>{tocando=true;ui();avisoMudo()};au.onpause=()=>{tocando=false;ui()};au.onwaiting=()=>{if(tocando)msg("Carregando…")};
$r("rplay").onclick=()=>{if(fallback){msg("Use o player oficial abaixo");return}
 if(tocando){pausar();ls("radioAuto","0")}else{ls("radioAuto","1");aoVivo();tocar().catch(()=>msg("Áudio bloqueado · clique em ▶"))}};
$r("rmute").onclick=()=>{au.muted=!au.muted;ui();avisoMudo()};
$r("rvol").oninput=e=>{au.volume=e.target.value/100;au.muted=false;ls("radioVol",e.target.value);ui();avisoMudo()};
document.querySelectorAll(".rband button").forEach(b=>b.onclick=()=>{const id=b.dataset.r;if(id===rid&&!fallback)return;const era=tocando||ls("radioAuto")!=="0";
 carregar(id).then(ok=>{if(ok&&era){aoVivo();tocar().catch(()=>{})}})});
async function iniciar(){ui();const ok=await carregar(rid);if(!ok||ls("radioAuto")==="0")return;
 try{await tocar()}catch(e){msg("▶ Toque na página para ouvir");
  const ev=["pointerdown","keydown"],go=x=>{if(tocando||ls("radioAuto")==="0"){ev.forEach(t=>document.removeEventListener(t,go,true));return}
   if(x.target.closest&&x.target.closest("#radio"))return;ev.forEach(t=>document.removeEventListener(t,go,true));aoVivo();tocar().catch(()=>{})};
  ev.forEach(t=>document.addEventListener(t,go,true))}}
const mn=$r("rmin");function setMin(m){$r("radio").classList.toggle("min",m);mn.textContent=m?"+":"–";mn.setAttribute("aria-expanded",m?"false":"true");
 mn.setAttribute("aria-label",m?"Expandir player":"Minimizar player");mn.title=m?"Expandir":"Minimizar";ls("radioMin",m?"1":"0")}
mn.onclick=()=>setMin(!$r("radio").classList.contains("min"));setMin(ls("radioMin")==="1");
iniciar()})();
</script></body></html>
"""


@app.route("/")
def home():
    import html as _h
    contato = (' Para pedir a remoção de um conteúdo: <a href="mailto:%s">%s</a>.' % (_h.escape(EMAIL_CONTATO), _h.escape(EMAIL_CONTATO))) if EMAIL_CONTATO else ""
    return PAGINA.replace("__NOME__", _h.escape(NOME_SITE)).replace("__CONTATO__", contato)


preparar_banco()
# A coleta NÃO liga aqui: só no processo que atende visitas (primeira requisição),
# para não haver duas coletas disputando o banco quando o servidor carrega o app em 2 processos.

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)
