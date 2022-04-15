import sys
import csv
"""
The program takes two csv files read by tools from csv module. Then it stores data in dictonary where each key is the row correspond to the join column and the values it stores, are the remaining column values.
The rows are stored in this fashion Dict[key] = [[col1, col2, col3], [col1, col2, col3], [col1, col2, col3]], by this manner we can stored even duplicated data. The stored data in dictionary can be searched really 
quickly because each value correspond to the key is described in memory by hash function. To avoid memory error in the program, when handling huge csv files, each csv is read and precessed in chunks of fixed value.
The value of the chunk depends of the size of the header, because greater header generates bigger list which is stored in dictionary.
"""
"""
Tested cases handle by the program.

    Argument handling:
    - When not enough argument is passed in cmd program returns: "Invalid number of arguments"
    - When invalid path to csv program returns: 'Cannot open path'
    - When invalid join type (misspelling or 
    not supported by join function) is passed to the program, it returns: "Non valid join argument"
    - When invalid column name i.e. not present in both headers or passed to the program with wrong spelling, program returns: Not Valid Join Column: name_of_the_column
    - When none join argument is passed, the default join type in the program is left join
    - When the join type is spelled right, it will be handle by the program independently if its write with lower or upper case. The same as with join function.

    Csv handling:
    - When join column is not present in at least one of the csv file arguments program returns: Not Valid Join Column: name_of_the_column
    - When column names which are not join column are duplicated in both csv headers. The program will add at the end of their names additional letters. Column from the left csv input: _x
    Column from the right csv input: _y
    - When in both csv duplicated data is present, it will be handle by the program and present in each join type.
    - When the row of data is present only in one csv file during left or right join, it will received NaN values for each column from the csv file where the row is missing.
    - To avoid memory crash due to size of the input csv file , it is read and 
    processed in chunks, which can be modified inside the program. The size of the chunk is inversely proportional to the number of columns, because number of columns determine the size in memory 
    of stored values for each key in python dictonary. 

"""
def create_dict(data, header_index):
    """
    Function creating dictonary for each dataset.
    Returns dictonary.
    """        
    map_of_rows = dict()
        
    for data_elem in data:
        col = data_elem[header_index]
        data_elem.pop(header_index)
        add_value(map_of_rows, col, data_elem)

    return map_of_rows

def add_value(dict_obj, key, value):
    '''
    Adds a key-value pair to the dictionary.
    If the key already exists in the dictionary, 
    it will associate multiple values with that 
    key instead of overwritting its value
    '''
    if key not in dict_obj:
        dict_obj[key] = [value]
    elif isinstance(dict_obj[key], list):
        dict_obj[key].append(value)
    else:
        dict_obj[key] = [dict_obj[key], value]
        
def read_file_header(path_to_file, join_column):
    """
    Function reading csv file. Returns tuple containing: header of csv file,
    csv reader iterator and opened csv file that will be closed at the end of the
    join function.
    """
    file_to_read = open(path_to_file, newline='')
    reader_func = csv.reader(file_to_read)
    header = next(reader_func)# the first line is the header
    if join_column not in header:
        raise NameError('Not Valid Join Column: ' +  join_column)
        
    return header, reader_func, file_to_read
def header_set_index(header_A_arg, header_B_arg, join_column_arg, is_right_a):
    """
    Function reading csv file. Returns tuple containing: header of csv file,
    csv reader iterator and opened csv file that will be closed at the end of the
    join function.
    """  
    head_indx_A  = header_A_arg.index(join_column_arg)
    head_indx_B  = header_B_arg.index(join_column_arg)
    if is_right_a:
        header_A_arg.pop(head_indx_A)
    else:
        header_B_arg.pop(head_indx_B)
    header_set_arg= list(set(header_A_arg).intersection(header_B_arg))
    return header_set_arg, head_indx_A, head_indx_B 

def header_handler(header_A, header_B, join_column, is_right):
    """
    Function handling with duplicated column names.
    Duplicated column name will received additional letters at the end.
    Column from the left csv input: _x
    Column from the right csv input: _y
    """      
    header_set, head_indx_A, head_indx_B = header_set_index(header_A, header_B, join_column, is_right)
    condition = lambda x, set_arg: x in set_arg
    add_A = "_x"
    add_B = "_y"
        
    if is_right:
        add_A, add_B, = add_B, add_A
            
    header_A = [header_elem + add_A if condition(header_elem, header_set) else header_elem for header_elem in header_A]
    header_B = [header_elem + add_B if condition(header_elem, header_set) else header_elem for header_elem in header_B]
    return header_A, header_B, head_indx_A, head_indx_B

def write(dict_A, dict_B, header_A, header_B, join_col_index_A, join_col_index_B, count, inner, is_right):
    """
    Writing join table to the stdout. Type of the join depends of the boolean values: inner and is_right.
    """

    if count == 0:
        if is_right:
            join_col_index_A, join_col_index_B = join_col_index_B, join_col_index_A
            header_A, header_B = header_B, header_A
            
        output_header = header_A + header_B
        print(*output_header, sep = ", ")


    for key1, value1 in dict_A.items():
        for val_el in value1:
            if key1 in dict_B.keys():
                for it in dict_B[key1]:
                    if is_right:
                        val_el, it = it, val_el
                    write_row = val_el +  it
                    write_row.insert(join_col_index_A, key1)
                    print(*write_row, sep = ", ")
            elif not inner:
                if is_right:
                    write_row  = ['NaN']*(len(header_B)) + val_el 
                else:
                    write_row = val_el + ['NaN']*(len(header_B))
                write_row.insert(join_col_index_A, key1)
                print(*write_row, sep = ", ")



def join(path_1, path_2, column, join_type = 'left'):                            
    if join_type == 'inner':
        inner_condition = True
        is_right_arg = False
    elif join_type == 'right':
        inner_condition = False
        is_right_arg = True
        path_1,path_2 = path_2,path_1
    elif join_type == 'left':
        is_right_arg = False
        inner_condition = False
    else:
        print("Non valid join argument")
        return
        

    #reading file_1 and extracting header and rows of data
    # Checking if join column is present in each header
    try:
        header_1, reader_1, file_1 = read_file_header(path_1, column)
    except NameError as err:
        print(err)
        return
    except OSError:
        print('Cannot open path', path_1)
        return

    #reading file_2 and extracting header and rows of data
    # Checking if join column is present in each header
    try:
        header_2, reader_2, file_2 = read_file_header(path_2, column)
    except NameError as err:
        print(err)
        return
    except OSError:
        print('Cannot open path', path_2)
        return

    #Handling headers
    header_1, header_2, join_col_head_indx_1, join_col_head_indx_2 = header_handler(header_1, header_2, column, is_right_arg)
    
    #Setting chunksize to avoid memomory error for big csv files
    chunksize = 6000 / int(len(max(header_1, header_2)))

    chunk_1, chunk_2, counter = [], [] , 0
    
    #Reading and joining csv for each chunk
    for ((i, line1), (j, line2)) in zip(enumerate(reader_1), enumerate(reader_2)) :
        if (i % chunksize == 0 and i > 0):
            #Creating Dict1
            dict_1 = create_dict(chunk_1, join_col_head_indx_1)
            
            #Creating Dict2
            dict_2 = create_dict(chunk_2, join_col_head_indx_2)
            
            #Write the join data 
            write(dict_1, dict_2, header_1, header_2, join_col_head_indx_1, join_col_head_indx_2, counter, inner_condition, is_right_arg)
            counter += 1
            
            #Free the memory after each chunk computation
            del chunk_1[:]  
            del dict_1 
            del chunk_2[:]  
            del dict_2 
            
        chunk_1.append(line1)
        chunk_2.append(line2)
        
    #Compuation the remaining piece of csv   
    dict_1 = create_dict(chunk_1, join_col_head_indx_1)
    dict_2 = create_dict(chunk_2, join_col_head_indx_2)
    
    write(dict_1, dict_2, header_1, header_2, join_col_head_indx_1, join_col_head_indx_2, counter, inner_condition, is_right_arg)
    
    #Closing files
    file_1.close()
    file_2.close()
   




def main():
    if len(sys.argv) < 5 or len(sys.argv) > 6 :
        print("Invalid number of arguments")
    else: 
        function = sys.argv[1].lower()
        if function == "join":
            if len(sys.argv) ==  5:
                arg1 = str(sys.argv[2])
                arg2 = str(sys.argv[3])
                arg3 = str(sys.argv[4])
                join(arg1, arg2, arg3)

            elif len(sys.argv) ==  6:
                arg1 = str(sys.argv[2])
                arg2 = str(sys.argv[3])
                arg3 = str(sys.argv[4])
                arg4 = str(sys.argv[5]).lower()
                join(arg1, arg2, arg3, arg4)



if(__name__ == "__main__"):
    main()