# Проектное задание. Data Engineering

## Задание 1
В некотором банке внедрили новую frontend-систему для работы с клиентами, а так же обновили и саму базу данных. Большую часть данных успешно были перенесены из старых БД в одну новую централизованную БД.  Но в момент переключения со старой системы на новую возникли непредвиденные проблемы в ETL-процессе, небольшой период (конец 2017 начало 2018 года) так и остался в старой базе. Старую базу отключили, а не выгруженные данные сохранили в csv-файлы. Недавно банку потребовалось построить отчёт по 101 форме. Те данные что остались в csv-файлах тоже нужны. Загрузить их в новую БД не получиться из-за архитектурных и управленческих сложностей, нужно рассчитать витрину отдельно. Но для этого сначала нужно загрузить исходные данные из csv-файлов в детальный слой (DS) хранилища в СУБД PostgreSQL.

 

## Задача 1.1

Разработать ETL-процесс для загрузки «банковских» данных из csv-файлов в соответствующие таблицы СУБД PostgreSQL. Покрыть данный процесс логированием этапов работы и всевозможной дополнительной статистикой (на ваше усмотрение). Обратите внимание, что в разных файлах может быть разный формат даты, это необходимо учитывать при загрузке.

## Задача 1.2

После того как детальный слой «DS» успешно наполнен исходными данными из файлов – нужно рассчитать витрины данных в слое «DM»: витрину оборотов (DM.DM_ACCOUNT_TURNOVER_F) и витрину остатков (DM.DM_ACCOUNT_BALANCE_F).

## Задача 1.3

После того как в предыдущих заданиях вы загрузили необходимую информацию и рассчитали витрины с оборотами и остатками, необходимо произвести расчет 101 формы за январь 2018 года.
101 форма содержит информацию об остатках и оборотах за отчетный период, сгруппированных по балансовым счетам второго порядка. Вам необходимо создать процедуру расчета, которая должна иметь один входной параметр – отчетную дату (i_on_date). Отчетная дата – это первый день месяца, следующего за отчетным. То есть, если мы хотим рассчитать отчет за январь 2018 года, то должны передать в процедуру 1 февраля 2018 года. В отчет должна попасть информация по всем счетам, действующим в отчетном периоде, группировка в отчете идет по балансовым счетам второго порядка (балансовый счет второго порядка – это первые 5 символов номера счета).

## Задача 1.4

Выполнив предыдущие 2 задачи вам удалось рассчитать отчётную форму 101. Менеджер проекта доволен, ЦБ получил отчёт, руководство банка тоже довольно. Теперь необходимо выгрузить эти данные в формате, который бы позволил легко обмениваться ими между отделами. Один из таких форматов – CSV.

Напишите скрипт, который позволит выгрузить данные из витрины «dm. dm _f101_round_f» в csv-файл, первой строкой должны быть наименования колонок таблицы.
Убедитесь, что данные выгружаются в корректном формате и напишите скрипт, который позволит импортировать их обратно в БД. Поменяйте пару значений в выгруженном csv-файле и загрузите его в копию таблицы 101-формы «dm. dm _f101_round_f_v2».

## Задание 2
После успешного формирования отчетов, к банку пришел заказчик данных и сообщил, что по имеющимся витринам имеются дефекты. Для того, чтобы заказчик был доволен и мог беспрепятственно пользовать витринами, необходимо оперативно устранить имеющиеся дефекты.

## Задача 2.1
Имеется витрина dm.client, в которой содержится различная информация по клиентам банка. Заказчик сообщил, что в таблице имеются дубли, которые нужно устранить.
Необходимо подготовить запрос, по которому можно обнаружить все дубли в витрине и удалить их.
