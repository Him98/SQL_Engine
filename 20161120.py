import os,re,sys
import pprint
import sqlparse
from collections import defaultdict
from prettytable import PrettyTable
from itertools import product

schema = {}
dataset = defaultdict(list)
agg_functions = ("avg", "sum", "max","distinct", "min")
keywords = ("select", "from", "where")
opt = ("=",">","<",">=","<=")

class UdError(Exception):
    def __init__(self,msg = "An error occured"):
        self.msg = msg
        print(msg)
        exit(-1)

def createTables(filename=None):
    if filename == None:
        print("Error: No metadata filename specified")
        return
    f = open(filename,"r")
    line = f.readline().strip()
    while line:
        if line == "<begin_table>":
            table_name = f.readline().strip()
            schema[table_name] = ()
            colName = f.readline().strip()
            while colName != "<end_table>":
                schema[table_name] += (table_name+"."+colName,)
                colName = f.readline().strip()
        line = f.readline().strip()

def loadingData(filename=None):
    curr_path = os.path.dirname(os.path.realpath(__file__))
    dirs = os.listdir(curr_path)
    for files in dirs:
        # print(files)
        table_name = files.split(".")[0]
        if(len(files.split("."))>1 and files.split(".")[1] == 'csv'):
            f = open(files,"r")
            line = f.readline().strip()
            while line:
                data = line.split(',')
                if(len(data) < 0):
                    line = f.readline().strip()
                    continue
                if(len(data) != len(schema[table_name])):
                    raise UdError("ERROR: Number_of_columns_differ_in_input_file")
                tmp = {}
                for i in range(len(schema[table_name])):
                    for j in range(len(data)):
                        if(i == j):
                            tmp[schema[table_name][i]] = data[j]
                dataset[table_name].append(tmp)
                line = f.readline().strip()
                # print(line)

def calculateAggregation(agg_colName,print_dataset,agg_funcName,tmp_schema):
    if(agg_funcName == "sum"):
        tot = 0
        for i in print_dataset:
            tot += i[agg_colName]
        new_name = agg_funcName+'('+agg_colName+')'
        return [new_name],[{new_name:tot}]
    elif(agg_funcName == "avg"):
        tot = 0
        for i in print_dataset:
            tot += i[agg_colName]
        new_name = agg_funcName+'('+agg_colName+')'
        return [new_name],[{new_name:((tot*1.0)/len(print_dataset))}]
    elif(agg_funcName == "max"):
        ma = 0
        # print(print_dataset)
        # print("Hi",agg_colName)
        for i in print_dataset:
            # print(i[agg_colName])
            ma = max(ma,int(i[agg_colName]))
        new_name = agg_funcName+'('+agg_colName+')'
        return [new_name],[{new_name:ma}]
    elif(agg_funcName == "min"):
        mi = print_dataset[0][agg_colName]
        for i in print_dataset:
            mi = min(mi,i[agg_colName])
        new_name = agg_funcName+'('+agg_colName+')'
        return [new_name],[{new_name:mi}]
    elif(agg_funcName == "distinct"):
        # print('Hi')
        vis = []
        mod_data = []
        # print(agg_colName)
        # new_name = agg_funcName+'('+agg_colName+')'
        # print(print_dataset)
        for i in print_dataset[:]:
            var = ()
            tmp_dict = {}
            for name_col in agg_colName:
                tmp_dict[name_col]=i[name_col][:]
                # print(i[name_col][:])
                var+=(i[name_col][:],)
            if(var in vis):
                continue
            vis.append(var)
            # i[new_name] = i.pop(agg_colName)
            mod_data.append(tmp_dict)
        tmp = []
        for c in tmp_schema:
            for name_col in agg_colName:
                if name_col == c:
                    tmp.append(name_col)
            # else:
            #     tmp.append(c)
        # print("TMP",tmp)
        # print("MOD",mod_data)
        return tmp,mod_data

def actualColumnName(columnName, tables):
    if re.search(r"[A-Za-z0-9]*[\.][A-Z]",columnName):
        fl = 0
        for tab in tables:
            if columnName in schema[tab]:
                fl = 1
                break
        if fl == 0:
            raise UdError("ERROR 1054 (42S22): Column_not_present_in_table")
        return columnName
    names = ""
    cnt = 0
    for t in tables:
        if (t + "." + columnName) in schema[t]:
            names += t
            cnt += 1
        if cnt>1:
            raise UdError('ERROR: Conflict_with_column_name')
    if cnt == 0:
        raise UdError('ERROR 1054 (42S22): Column_not_present_in_table')
    return (names+"."+columnName)

def printingData(tmp_schema,col,agg_map,print_dataset,wh,arr,req_tables):
    fields = []
    fl = 0
    # print(tmp_schema)
    vis = []
    if(wh > 0):
        tmp = ()
        cnt = 0
        for i in arr:
            cnt = 0
            for j in tmp_schema:
                if(i[0] == j or i[1] == j):
                    break
                else:
                    cnt += 1
            tmp_schema = tmp_schema[:cnt] + tmp_schema[cnt+1:]
        # print(tmp_schema)
    if "*" in col:
        fields = list(tmp_schema)
    else:
        if len(agg_map) > 1:
            raise UdError("ERROR: multiple_aggregations_not_allowed")
        elif len(agg_map) == 1:
            (agg_colName, agg_funcName) = agg_map[0]
            if (agg_funcName == 'distinct'):
                tmp_lst = agg_colName[9:len(agg_colName)-1].split(',')
                # print(tmp_lst)
                name_col = []
                for i in range(0,len(tmp_lst)):
                    name_col.append(actualColumnName(tmp_lst[i],req_tables))
                agg_colName = name_col
            fields, print_dataset = calculateAggregation(agg_colName,print_dataset,agg_funcName,tmp_schema)
            # print(fields)
            # print(print_dataset)
        else:
            for c in col:
                if c != "*":
                    fields.append(c)

    # final_table = PrettyTable(fields)
    # print("Final",print_dataset)
    # for i in print_dataset:
    #     tmp_list = []
    #     for field in final_table.field_names:
    #         tmp_list.append(i[field])
    #     final_table.add_row(tmp_list)

    # print(final_table)
    for i in range(len(fields)):
        if i < len(fields)-1:
            print(fields[i],end = ','),
        else:
            print(fields[i])
    for i in print_dataset:
        for field in range(len(fields)):
            if field < len(fields)-1:
                print(i[fields[field]],end = ','),
            else:
                print(i[fields[field]])
    return print_dataset

def queryHelper(query):
    # print(query)
    req_col = []
    req_tables = []
    agg_map = []
    req_conditions = []
    try:
        dis = 0
        comm = query.split("\n")
        for i in comm:
            # print(i)
            if('distinct' in i):
                dis = 1
            if "AND" in i or "OR" in i:
                comm1 = i.replace(" ","")
                comm1 = comm1.split("AND")
                comm1 = ["AND"] + comm1
                # print("Main1",comm1)
            elif "and" in i or "or" in i:
                comm1 = i.replace(" ","")
                comm1 = comm1.split("and")
                comm1 = ["and"] + comm1
                # print("Main2",comm1)
            else:
                comm1 = i.split()
            if len(comm1) >= 2 and comm1[0] in keywords:
                flag = comm1[0]
            if(flag == "select"):
                names = comm1[-1].strip(",").strip()
                while (names[0] == '\"' or names[0] == '\'') and len(names) > 1 and names[0] == names[-1]:
                    names = names[1:-1]
                req_col.append(names)
                # print(req_col)

            elif(flag == "from"):
                if(comm1[0] == "from"):
                    names = " ".join(comm1[1:]).strip(",")
                    while (names[0] == '\"' or names[0] == '\'') and len(names) > 1 and names[0] == names[-1]:
                        names = names[1:-1]
                    req_tables.append(names)
                else:
                    names = " ".join(comm1).strip(",")
                    while (names[0] == '\"' or names[0] == '\'') and len(names) > 1 and names[0] == names[-1]:
                        names = names[1:-1]
                    req_tables.append(names)
                # print(req_tables)

            elif(flag == "where"):
                if(comm1[0][:] == "where"):
                    req_conditions.append("".join(comm1[1:]))
                else:
                    req_conditions.append(" ".join(comm1[:]))
                # print(req_conditions)
            else:
                raise UdError("ERROR: Invalid Query")

        if len(req_tables) == 0 and (query.split('\n')[-1]).split(" ")[-1] == 'from':
            raise UdError('ERROR: No tables given after from')
        elif len(req_tables) == 0:
            raise UdError('ERROR: invalid query')

        for i in req_tables:
            if i not in schema:
                raise UdError('ERROR 1146 (42S02): table name %s doesn\'t exist' %(i))

        if 'where' in query and len(req_conditions) == 0:
            raise UdError('ERROR: No conditions given after where')

        if 'where' in query:
            for i in req_conditions:
                if i not in ['and','or'] and len(i)<3:
                    raise UdError('ERROR: Give proper conditions given after where')            

        brackets = re.compile(r'(\(|\))')
        tmp_cond = []
        req_conditions = re.sub(brackets, r' \1 ', " ".join(req_conditions)).split()
        for i in req_conditions:
            if i not in ("and", "or", "(", ")"):
                tmp_cond.append(tuple(re.sub(re.compile(r'(<|>|<=|>=|=)'), r' \1 ', i).split()))
            else:
                tmp_cond.append(i)
        req_conditions = tmp_cond
        # req_conditions = [tuple(re.sub(re.compile(r'(<|>|<=|>=|=)'), r' \1 ', i).split()) if i not in ("and", "or", "(", ")") else i for i in req_conditions]

        if dis == 1:
            tmp_lst1 = "distinct("
            for i in req_col:
                tmp_lst1 += i
                tmp_lst1 += ','
            tmp_lst1=tmp_lst1[:-1] + ')'
            req_col = [tmp_lst1]

        # print(req_col)
        colPrint = []
        for col in req_col:
            if( col == "*"):
                pass
            elif '.' not in col:
                # print(req_tables)
                tmp = []
                for tab in req_tables:
                    if (tab+"."+col) in schema[tab]:
                        tmp.append(tab)
                if(len(tmp) > 1):
                    raise UdError("ERROR: Conflict_with_column_name")
                elif len(tmp) == 0:
                    func_name = ""
                    for func in agg_functions:
                        if(func == 'distinct' and dis == 1):
                            tmp_lst = col[9:len(col)-1].split(',')
                            # print(tmp_lst)
                            name_col = []
                            for i in range(1,len(tmp_lst)):
                                name_col.append(tmp_lst[i])
                        else:
                            expr = re.compile(r'(%s)\(([A-Za-z])\)' %(func))
                            # print(expr)
                            name_col = re.sub(expr,r'\2',col)
                        # print(name_col)
                        # print(col)
                        func_name = func
                        if(col != name_col):
                            break
                    if col == name_col or func_name == "":
                        # print(col)
                        # print(name_col)
                        raise UdError("ERROR 1054 (42S22): Column_not_present_in_table")
                    if func_name != 'distinct':    
                        col = actualColumnName(name_col,req_tables)
                    agg_map.append((col,func_name))
                    # print(agg_map)
                elif len(tmp) == 1:
                    col = ".".join([tmp[0],col])
            elif '.' in col:
                fl = 0
                # print(col)
                for tab in req_tables:
                    if col in schema[tab]:
                        fl = 1
                        break
                if fl == 0:
                    raise UdError("ERROR 1054 (42S22): Column_not_present_in_table")
                colPrint.append(col)
                continue
            else:
                if col not in schema[tab]:
                    raise UdError("ERROR 1054 (42S22): Column_not_present_in_table")
            colPrint.append(col)
        col = colPrint

        tmp_dataset = [{}]
        tmp_schema = tuple()
        for t in req_tables:
            tmp_schema += schema[t]

        num = 0

        for t in req_tables:
            num += len(schema[t])
            for i1, i2 in product(dataset[t], tmp_dataset):
                merged = {}
                merged.update(i1)
                merged.update(i2)
                tmp_dataset.append(merged)
            if(tmp_dataset[0] == dict()):
                del tmp_dataset[0]
        # print(len(tmp_dataset))
        tmp1 = []
        for i in range(len(tmp_dataset)):
            if len(tmp_dataset[i]) == num:
                tmp1.append(tmp_dataset[i])
        tmp_dataset = tmp1
        # print(tmp_dataset)
        
        # for i in tmp_dataset:
        #     print(i)
        # print(len(tmp_dataset))

        #Applying th final conditions
        # print(req_conditions)
        wh = 0
        arr = []
        print_dataset = []
        for r in tmp_dataset:
            cond = []
            for c in req_conditions:
                tabCol2 = ""
                if c in ("AND","OR",")","(","and","or"):
                    cond.append(c.lower())
                else:
                    dt = ""
                    # print(c)
                    [tabCol1, op, tabCol2] = c
                    tabCol1 = actualColumnName(tabCol1,req_tables)
                    tabCol2 = tabCol2.strip()
                    while len(tabCol2[:]) > 1 and (tabCol2[0] == '\"' or tabCol2[0] == '\''):
                        if (tabCol2[0] == tabCol2[-1]):
                            tabCol2 = tabCol2[1:-1]
                        else:
                            break
                    if (not (tabCol2.isdigit())) and tabCol2[0]!='-':
                        tabCol2 = actualColumnName(tabCol2,req_tables)
                        dt = r[tabCol2]
                    else:
                        dt = (tabCol2)
                        # print(dt)
                    if op == "=" and int(r[tabCol1]) == int(dt):
                        cond.append("True")
                        if(('.' in tabCol2) and ([tabCol1,tabCol2] not in arr)):
                            wh += 1
                            arr.append([tabCol1,tabCol2])
                    elif op == ">" and int(r[tabCol1]) > int(dt):
                        cond.append("True")
                        # if(('.' in tabCol2) and ([tabCol1,tabCol2] not in arr)):
                        #     wh += 1
                        #     arr.append([tabCol1,tabCol2])
                    elif op == "<" and int(r[tabCol1]) < int(dt):
                        cond.append("True")
                        # if(('.' in tabCol2) and ([tabCol1,tabCol2] not in arr)):
                        #     wh += 1
                        #     arr.append([tabCol1,tabCol2])
                    elif op == ">=" and int(r[tabCol1]) >= int(dt):
                        cond.append("True")
                        # if(('.' in tabCol2) and ([tabCol1,tabCol2] not in arr)):
                        #     wh += 1
                        #     arr.append([tabCol1,tabCol2])
                    elif op == "<=" and int(r[tabCol1]) <= int(dt):
                        cond.append("True")
                        # if(('.' in tabCol2) and ([tabCol1,tabCol2] not in arr)):
                        #     wh += 1
                        #     arr.append([tabCol1,tabCol2])
                    elif op not in opt:
                        raise UdError("ERROR: Invalid Operator given.")
                        cond.append("False")
                    else:
                        cond.append("False")
            # print(cond)
            if( len(cond) > 0):
                ans_str = eval(" ".join(cond))
            if(len(cond)>0 and ans_str):
                # print(r)
                print_dataset.append(r)
            elif(len(cond) == 0):
                print_dataset.append(r)
        # print("Check",arr)

    except Exception as e:
        
        [exc_type, exc_obj, exc_tb] = sys.exc_info()
        print ("ERROR:", str(exc_type), "on", exc_tb.tb_lineno)
        exit(-1)

    return tmp_schema,col,agg_map,print_dataset,wh,arr,req_tables


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('ERROR: invalid_arguments')
        print('USAGE: python3 %s \'SQL query\''%(sys.argv[0]))
        exit(-1)
    createTables("metadata.txt")
    # print(schema)
    loadingData()
    # for i in dataset['table1']:
    #     print(i)
    # for i in dataset['table2']:
    #     print(i)
    # print(dataset)

    try:
        inp_comm = sys.argv[1]
        inp_comm = inp_comm.strip()
        if inp_comm[len(inp_comm)-1] != ";":
            print('ERROR: semicolon missing')
            exit(-1)
        # while inp_comm[len(inp_comm)-1] != ";":
        #     inp_comm += " " + input(".. ")
        #     inp_comm.strip()
        if inp_comm.lower() == 'exit;':
            exit(-1)
        inp_comm = inp_comm[0:-1]
        parsed_comm = sqlparse.format(inp_comm, keyword_case='lower', reindent=True)
        tmp_schema,col,agg_map,print_dataset,wh,arr,req_tables = queryHelper(parsed_comm)
        dataToPrint = printingData(tmp_schema,col,agg_map,print_dataset,wh,arr,req_tables)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        exit(-1)
    except EOFError:
        print("End of File")
    except UdError as exc:
        print(exc.msg)