Делаем докер композ ап, на http://localhost:9200 эластиксерч возвращает json с именем кластера, на http://localhost:5601 будет кибана, это юишка, чтобы покидать запросы эластику, там в дев тулз заходим. В качестве примера 2 книжки и 3 чанка было, я такие запросы кидала:

GET /books/_count
возвращает количество документов в индексе books (сколько книг в каталоге)

GET /book_content/_count
возвращает количество документов в индексе book_content (сколько чанков текста книг)

GET /books/_doc/BOOK1
получает документ с id=BOOK1 из индекса books, даёт посмотреть исходные поля книги (title, authors, genres, description, suggest)

POST /books/_search
{
  "query": {
    "multi_match": {
      "query": "Moby Melville",
      "fields": ["title^4","authors^3","description"]
    }
  }
}
делаем полнотекстовый поиск по нескольким полям индекса books, поля: title^4 — поле title с повышенным весом (буст 4), authors^3 — поле authors с повышенным весом (буст 3), description — обычный вес
тут эластик использует BM25, совпадения по title и authors влияют сильнее из-за бустов. так у нас сейчас происходит глобальный поиск по каталогу (юзкейс 3)

POST /book_content/_search
{
  "query": {
    "bool": {
      "filter": [{ "term": { "book_id": "BOOK1" } }],
      "must": { "match": { "text": "Ishmael" } }
    }
  },
  "highlight": { "fields": { "text": { "fragment_size": 140, "number_of_fragments": 3 } } }
}
ищет в индексе book_content только по документам, где book_id=BOOK1. filter это жёсткая фильтрация (не влияет на скоринг), оставляет только нужную книгу, must это условия полнотекстового поиска (влияют на релевантность). highlight вернет фрагменты текста с подсветкой совпадений (до 3 фрагментов по ~140 символов). пока у меня ненастоящие книжки, скажем так, поэтому на это пофиг, но это для юзкейса 13

POST /books/_search
{
  "suggest": {
    "title-suggest": { "prefix": "mob", "completion": { "field": "suggest" } }
  }
}
автодополнение по полю suggest в индексе books. туь completion хранит структуры для быстрых подсказок, возвращает варианты подсказок и связанные документы

POST /books/_search
{
  "query": {
    "multi_match": {
      "query": "whales",
      "fields": ["title^4","authors^3","description"]
    }
  },
  "highlight": { "fields": { "description": {} } }
}
пытается слово whales в каталоге книг найти, подсвечивает соответствующие места в description. у меня слово в метаданных не встречается, поэтому нет хитсов