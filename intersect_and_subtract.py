__version__ = 'V4.0'

print('''
Программа пересечения и вычитания.

Работает только с testing-версией ClickHouse!

Автор: Платон Быкадоров (platon.work@gmail.com), 2019.
Версия: V4.0.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/index-tools/blob/master/README.md

Обязательно!
Перед запуском программы нужно установить модуль:
pip3 install clickhouse-driver --user

Таблицы, по одному из столбцов которых
предполагается пересекать или вычитать,
должны соответствовать таким требованиям:
1. Если их несколько, то одинаковой структуры;
2. Содержать шапку (одну и ту же для всех);
3. Каждая по отдельности - сжата в GZIP/BGZIP.
Пример подходящих данных - *.egenes.txt.gz-файлы проекта GTEx:
https://storage.googleapis.com/gtex_analysis_v8/single_tissue_qtl_data/GTEx_Analysis_v8_eQTL.tar

--------------------------------------------------

Пересечение:
Столбец *каждой* левой таблицы пересекается
с соответствующим столбцом *всех* правых таблиц.

Жёсткий режим пересечения:
*Остаются* только те ячейки столбца левой таблицы,
для которых *есть совпадение* в соответствующем
столбце *всех* правых таблиц.

Щадящий режим пересечения:
*Остаются* только те ячейки столбца левой таблицы,
для которых *есть совпадение* в соответствующем
столбце *хотя бы одной* правой таблицы.

--------------------------------------------------

Вычитание:
Из столбца *каждой* левой таблицы вычитается
соответствующий столбец *всех* правых таблиц.

Жёсткий режим вычитания:
*Остаются* только те ячейки столбца левой таблицы,
для которых *нет совпадения* в соответствующем
столбце *всех* правых таблиц.

Щадящий режим вычитания:
*Остаются* только те ячейки столбца левой таблицы,
для которых *нет совпадения* в соответствующем
столбце *хотя бы одной* правой таблицы.

--------------------------------------------------

Если настройки, запрашиваемые в рамках интерактивного
диалога, вам непонятны - пишите, пожалуйста, в Issues.
''')

print('\nИмпорт модулей программы...')

import sys

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from backend.table_indexer import create_database
from clickhouse_driver import Client
import copy, os, gzip

#Индексация выбранных пользователем
#столбцов исходных сжатых таблиц.
#При этом образуется БД, включающая
#в себя содержимое этих столбцов и
#индексы, представляющие собой байтовые
#позиции начала всех табличных строк.
#Получение путей к папке с упомянутыми
#таблицами и конечной папки, а также
#различных характеристик базы данных.
arc_dir_path, trg_dir_path, db_name, tab_names, col_names_n_types = create_database()

if len(tab_names) == 1:
        print('Для пересечения или вычитания требуется не менее двух таблиц')
        sys.exit()
        
#Стандартные действия для подключения к БД.
client = Client('localhost')
client.execute(f'USE {db_name}')

left_tab_names = input(f'''\nИмя одной или имена нескольких "левых" таблиц БД
(через запятую с пробелом)
(игнорирование ввода ==> все таблицы БД сделать "левыми")
[...|<enter>]: ''').split(', ')
if left_tab_names == ['']:
        left_tab_names = copy.deepcopy(tab_names)
        
right_tab_names = input(f'''\nИмя одной или имена нескольких "правых" таблиц БД
(через запятую с пробелом)
(игнорирование ввода ==> все таблицы БД сделать "правыми")
[...|<enter>]: ''').split(', ')
if right_tab_names == ['']:
        right_tab_names = copy.deepcopy(tab_names)
        
if len(col_names_n_types) > 1:
        col_name = input('\nИмя столбца, по которому пересекаем или вычитаем: ')
        if col_name not in col_names_n_types:
                print(f'{col_name} - недопустимая опция')
                sys.exit()
else:
        col_name = list(col_names_n_types.keys())[0]
        
action = input('''\nПересекать или вычитать табличные данные?
[intersect(|i)|subtract(|s)]: ''')
if action in ['intersect', 'i']:
        action, sign, right_tab_dest = 'int', '&', 'IS NOT NULL'
elif action in ['subtract', 's']:
        action, sign, right_tab_dest = 'sub', '-', 'IS NULL'
else:
        print(f'{action} - недопустимая опция')
        sys.exit()
        
mode = input('''\nЖёсткий или щадящий режим?
[hard(|h)|gentle(|g)]: ''')
if mode in ['hard', 'h']:
        logical = " AND "
elif mode in ['gentle', 'g']:
        logical = " OR "
else:
        print(f'{mode} - недопустимая опция')
        sys.exit()
        
client.execute('SET join_use_nulls = 1')

#Созданная бэкендом БД также
#содержит отдельную таблицу с
#элементами пантабличной шапки.
#Исходная последовательность этих
#элементов сохранена, поэтому из
#них легко собраем шапку обратно.
header_line = '\t'.join([tup[0] for tup in client.execute('SELECT header_cells FROM header')])

#Все дальнейшие действия будут производиться
#для каждой левой таблицы по-отдельности.
for left_tab_name in left_tab_names:
        
        #Имена таблиц базы данных происходят от имён
        #соответствующих индексированных сжатых таблиц.
        #При формировании последних приходилось заменять
        #точки и дефисы на определённые кодовые слова.
        #Теперь для решения обратной задачи - получения
        #имени архива по имени таблицы - возвращаем точки
        #и дефисы на позиции альтернативных обозначений.
        #Если имя правой таблицы по ошибке или специально
        #совпадает с именем левой, то будет проигнорировано.
        left_arc_file_name = left_tab_name[3:].replace('DOT', '.').replace('DEFIS', '-')
        right_arc_file_names = [right_tab_name[3:].replace('DOT', '.').replace('DEFIS', '-') \
                                for right_tab_name in right_tab_names if right_tab_name != left_tab_name]
        
        #Конструируем имя конечного файла
        #и абсолютный путь к этому файлу.
        trg_file_name = f'{action}_res_{".".join(left_arc_file_name.split(".")[:-1])}'
        trg_file_path = os.path.join(trg_dir_path, trg_file_name)
        
        #Создаём флаг, по которому далее будет
        #определено, оказались ли в конечном
        #файле строки, отличные от хэдеров.
        empty_res = True
        
        #Открытие левого проиндексированного
        #архива на чтение и файла для
        #поисковых результатов на запись.
        with gzip.open(os.path.join(arc_dir_path, left_arc_file_name)) as left_arc_file_opened:
                with open(trg_file_path, 'w') as trg_file_opened:
                        
                        #Подготавливаем и прописываем первый хэдер.
                        #Им будет выражение, состоящее из имени
                        #архива, соответствующего текущей
                        #левой таблице, знака, представляющего
                        #выбранное действие, и перечисленных
                        #через запятую имён правых архивов.
                        trg_file_opened.write(f'##{left_arc_file_name} {sign} {", ".join(right_arc_file_names)}\n')
                        
                        #Второй хэдер - ранее восстановленная
                        #из специальной таблицы БД шапка.
                        trg_file_opened.write(header_line + '\n')
                        
                        #И пересечение, и вычитание программа
                        #выполняет с помощью ClickHouse-алгоритма
                        #левостороннего внешнего объединения.
                        #Если не применить какие-либо фильтры,
                        #алгоритм выдаст все элементы столбца
                        #таблицы, имя которого будет вписано
                        #между FROM и первым LEFT JOIN инструкции,
                        #формирование которой мы сейчас начали.
                        #Если для элемента этого (левого) столбца
                        #не обнаружится такого же элемента в
                        #оппонирующих (правых) столбцах, то взамен
                        #недостающих элементов выведется значение NULL.
                        #Для элемента левого столбца, может, наоборот,
                        #найтись более одного совпадения в правых.
                        #Тогда, в случае пересечения, строка
                        #или часть строки с этим элементом
                        #вылезет столько же раз, сколько и
                        #совпадений - таково поведение многих
                        #(а может, и всех) SQL-СУБД в этой ситуации.
                        #Подавляем появление таких повторов,
                        #вписывая ANY перед каждым LEFT.
                        #Но если внутри левого столбца дублирующиеся
                        #элементы есть изначально, ANY не сработает.
                        #И это правильно: дубли в столбце
                        #исходного файла вряд ли существуют
                        #без одобрения исследователя, а,
                        #значит, программа их должна сохранять.
                        left_join = [f'ANY LEFT JOIN {right_tab_name} ON {left_tab_name}.{col_name} = {right_tab_name}.{col_name}' \
                                     for right_tab_name in right_tab_names if right_tab_name != left_tab_name]
                        
                        #Пересечение будет считаться
                        #успешным, если в декартовом
                        #произведении таблиц для данного
                        #элемента левого столбца либо
                        #ни в одном из правых столбцов
                        #не окажется соответствующего
                        #NULL (если выбран жёсткий вариант),
                        #либо NULL займёт не все правые
                        #позиции (при щадящем варианте).
                        #В результате же вычитания, от
                        #левого столбца останутся либо
                        #те элементы, у которых NULL
                        #вылез во всех правых столбцах
                        #(жёсткий вариант), либо элементы
                        #с, как минимум, одним правым
                        #NULL (щадящий вариант).
                        where = [f'{right_tab_name}.{col_name} {right_tab_dest}' \
                                 for right_tab_name in right_tab_names if right_tab_name != left_tab_name]
                        
                        print(f'\nРабота с таблицей {left_tab_name} базы данных')
                        
                        #Инструкция, собственно,
                        #пересечения или вычитания.
                        #Результат - остающиеся байтовые
                        #позиции начала строк соответствующей
                        #(левой) архивированной таблицы.
                        res = client.execute_iter(f'''SELECT {left_tab_name}.line_start FROM {left_tab_name}
                                                      {" ".join(left_join)}
                                                      WHERE {logical.join(where)}''')
                        
                        print(f'Извлечение отобранных строк таблицы {left_arc_file_name}')
                        
                        #Перемещение курсора по сжатой таблице к
                        #началу каждой отвечающей запросу строки.
                        #Очередная новая позиция курсора отсчитывается
                        #не от начала файла, а от последней запомненной
                        #позиции, что в ряде случаев приводит к
                        #достижению колоссальной производительности.
                        #Прописывание отобранных строк в конечный файл.
                        #Присвоение флагу значения, показывающего
                        #наличие в конечном файле нехэдерных строк.
                        cur_pointer = 0
                        for line_start in res:
                                new_pointer = line_start[0]
                                left_arc_file_opened.seek(new_pointer - cur_pointer, 1)
                                trg_file_opened.write(left_arc_file_opened.readline().decode('UTF-8'))
                                cur_pointer = left_arc_file_opened.tell()
                                empty_res = False
                                
        #Если флаг-индикатор так и
        #остался равен True, значит,
        #результатов пересечения/вычитания
        #для данной левой таблицы нет, и в
        #конечный файл попали только хэдеры.
        #Такие конечные файлы программа удалит.
        if empty_res == True:
                os.remove(trg_file_path)
                
client.disconnect()
