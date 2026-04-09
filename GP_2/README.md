# ГП Смадимо

# Описание проекта

На блокчейне TON существуют .ton домены — цифровые активы, которые можно использовать для создания запоминающихся псевдонимов для крипто-адресов и для хостинга специальных анонимных и децентрализованных [TON-сайтов](https://ton.org/en/ton-sites). 

Похожие технологии есть на других блокчейнах: [Ethereum Name Service](https://ens.domains/), [Solana Name Service](https://www.sns.id/ru). Несмотря на то, что технически TON значительно более продвинутый, чем Эфириум или Солана, домены на TON гораздо менее популярны. На данный момент зарегистрировано всего 166 тысяч доменов .ton, против 452 тысяч в .sol и 3.5 млн .eth.

Однако, домены .ton имеют большой потенциал, и за последние два месяца спрос на них резко возрос, вместе с ростом сообщества инвесторов. При покупке доменного имени в сети TON было бы полезно сравнить его с соответствующими зарегистрированными именами в других сетях, чтобы предсказать возможную справедливую цену для перепродажи.

Для упрощения оценки стоимости приобретаемых активов, в маркетплейс .ton доменов [webdom.market](http://webdom.market) (наш формальный заказчик) планируется добавить отображение “ИИ-оценки” на основе данных по другим блокчейнам. Для решения этой задачи мы решили собрать датасет по доменам .eth и .sol, собрав по доменным именам следующие признаки:

- Имя (string)
- Длина (uint)
- Есть ли цифра (binary)
- Есть ли буква (binary)
- Есть ли дефис (binary)
- Семантическая категория (string)
- Цена регистрации в ENS (float $)
- Цена регистрации в SNS (float $)
- Дата регистрации в ENS (datetime)
- Дата регистрации в SNS (datetime)
- Количество вторичных продаж в ethereum и solana (int)
- Сумма сделок по вторичным продажам в ethereum и solana (float $)
- Популярность соответствующего домена в web2 (int)
- Целевая переменная: цена продажи в .ton

## Источники информации

| Параметр | Название источника | Способ получения | Примечание |
| --- | --- | --- | --- |
| Исторический курс ETH для конвертации в $  | Binance API | API эндпоинт [**`https://api.binance.com/api/v3/klines`**](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data) | Использован бесплатный API ключ платформы |
| Список всех .sol доменов с именем, ценой и датой регистрации | SNS Bonfida API | API эндпоинт [`https://sns-api.bonfida.com/sales/registrations`](https://docs.sns.id/dev/sns-api/sales#registrations) | Ресурс доступен без авторизации |
| Список всех .eth доменов с именем, ценой и датой регистрации | Thegraph API | API эндпоинт [`https://api.thegraph.com/subgraphs/name/ensdomains/ens`](https://thegraph.com/explorer/subgraphs/5XqPmWe6gjyrJtFn9cLy237i4cWw2j9HcUJEXsP5qGtH?view=Query&chain=arbitrum-one) | Не поняли как использовать API ключ (вероятно нужно задепать крипту для использования). Использовали эндпоинт без авторизации, с rate limits |
| Статистика вторичных продаж .sol доменов | SNS Bonfida API | API эндпоинт [`https://sns-api.bonfida.com/v2/domains/history`](https://docs.sns.id/dev/sns-api/domains#history) | Ресурс доступен без авторизации |
| Статистика вторичных продаж .eth доменов | Dune API | API эндпоинты 
[`https://api.dune.com/api/v1/sql/execute`](https://docs.dune.com/api-reference/executions/endpoint/execute-sql)
и
[`https://api.dune.com/api/v1/execution/{query_id}/results`](https://docs.dune.com/api-reference/executions/endpoint/get-execution-result) | Использован бесплатный API ключ |
| Семантическая категория | Kaggle + Curlie.org | 2 сырых датасета доступных для скачивания:
[URL Classification](https://www.kaggle.com/datasets/shaurov/website-classification-using-url)
и
[Curlie Directory Data](https://curlie.org/download) | Датасет curlie в очень неприменимом формате, был приведен к нормальному виду до этого проекта. В рамках проекта мы просто взяли готовый нормализованный датасет. |
| Популярность домена | CloudFlare Radar | @Гордей заполни | Скачены TOP-100, TOP-200, TOP-500, …, TOP-1M самых популярных доменов с [сайта](https://radar.cloudflare.com/domains). |
| Цена продажи в блокчейне TON | webdom.market | web-scraping страницы [https://webdom.market/analytics/history](https://webdom.market/analytics/history)  | Использована библиотека Selenium |
| Длина, наличие букв/цифр/дефисов | EDA | Признаки добавлены при обработке данных | — |

## Roadmap проекта 

- [x]  Подготовить описание проекта - Матвей Юдин
- [x]  Заресерчить способы получения данных - Матвей Юдин
- [ ]  Собрать все данные с помощью API — Марков Даниил
- [ ]  Собрать данные с помощью Selenium — Литвиненко Гордей
- [ ]  Провести EDA — Литвиненко Гордей
- [ ]  Подготовить презентацию — Вся команда