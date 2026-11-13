# Portal agregador de notícias

Coleta notícias de vários feeds RSS a cada 2 minutos, remove duplicadas e
atualiza a página ao vivo (sem recarregar).

## 0. Conferir se as fontes estão funcionando
Com o portal rodando, abra `/status` (ex.: http://localhost:5000/status). A tabela mostra, para cada
uma das 31 fontes, se o feed foi encontrado, quantas notícias entraram e qual endereço de feed está em uso.
O portal tenta o endereço informado em `fontes.json`, depois procura o feed na página inicial do site
e, por fim, testa caminhos comuns (/feed, /rss...). Fontes sem feed ficam como "SEM FEED encontrado"
(refaz a tentativa a cada 1 hora).

## 1. Trocar as fontes
Abra `fontes.json` num editor de texto (Bloco de Notas serve). Cada fonte tem nome, `site`, `feeds` (endereços de RSS, pode ficar vazio `[]`) e `categoria`.

- A última linha da lista NÃO leva vírgula no final.
- Para achar o RSS de um site, tente `site.com/feed`, `site.com/rss` ou procure "RSS" no rodapé.
- Os endereços de exemplo podem mudar com o tempo; teste cada um colando no navegador.

## 2. Rodar no seu computador (para testar)
1. Instale o Python em https://www.python.org/downloads/ (marque "Add Python to PATH").
2. Abra o terminal (Prompt de Comando) dentro desta pasta e rode:

       pip install -r requirements.txt
       python app.py

3. Abra http://localhost:5000 no navegador. Em ~30 segundos as notícias aparecem.

## 3. Colocar no ar (para todo mundo acessar)
Exemplo com o Render (https://render.com), que tem plano gratuito:
1. Crie uma conta no GitHub e envie esta pasta para um repositório novo (botão "Upload files").
2. No Render: New → Web Service → conecte o repositório.
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `gunicorn app:app --workers 1 --threads 50 --timeout 0`
5. Clique em Deploy. Você recebe um endereço público.

Importante: use sempre `--workers 1`. No plano gratuito o serviço "dorme" sem
visitas e o banco é apagado a cada reinício (ele só rebusca as notícias recentes).
Para uso sério, use um plano pago com disco persistente (ou um VPS barato).

## Ajustes opcionais (variáveis de ambiente)
- `NOME_SITE`: título do portal.
- `INTERVALO_SEGUNDOS`: de quanto em quanto tempo checar as fontes (padrão 120).

## Boas práticas
O portal mostra só título, resumo curto, miniatura e link para a matéria original,
com o nome da fonte. Não copie o texto completo das matérias.
