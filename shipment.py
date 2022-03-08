import sqlite3
import csv
con = sqlite3.connect('shipment_database.db')
cur = con.cursor()
i = -1
b = -1
a = -1
last_i = 0
last_j = 0

try:
    cur.execute("delete from shipment;")
    con.commit()
    cur.execute("delete from product;")
    con.commit()
except:
    print("Cannot delete record from database")

try:
    cur.execute("drop table product")
except:
    print('Cannot drop table')

sql_create_product_table = """ create table if not exists product 
                                    (id integer PRIMARY KEY,
                                    name text NOT NULL); """
                                    
try:
    cur.execute(sql_create_product_table)
except:
    print('Cannot create table')

with open('data/shipping_data_0.csv', newline='') as csvfile:
    reader = list(csv.reader(csvfile, delimiter='\t', quotechar='|'))
    for r in reader:
        print(r)
        stg = r[0]
        print('\n')
        i+=1
        if i == 0:
            continue
        string_list = stg.split(",")
        print(string_list)
        cur.execute("insert into shipment VALUES (?, ?, ?, ?, ?)", (i, i, string_list[4], string_list[0], string_list[1]))
        con.commit()
        last_i = i
    
with open('data/shipping_data_0.csv', newline='') as csvfile:
    reader2 = list(csv.reader(csvfile, delimiter='\t', quotechar='|'))
    for r in reader2:
        print(r)
        stg = r[0]
        print('\n')
        b+=1
        if b == 0:
            print(stg)
            continue
        string_list = stg.split(",")
        print(string_list)
        cur.execute("insert into product VALUES (?, ?)", (b, string_list[2]))
        con.commit()
        last_j = b

try:
    i = -1
    dict = {}
    with open('data/shipping_data_1.csv', newline='') as csvfile1, open('data/shipping_data_2.csv', newline='') as csvfile2:
        reader = list(csv.reader(csvfile1, delimiter='\t', quotechar='|'))
        reader2 = list(csv.reader(csvfile2, delimiter='\t', quotechar='|'))
        #t = (reader[0], reader[1],reader2[1], reader2[2])
        print(reader)
        for r in reader2:
            print(r)
            stg = r[0]
            print('\n')
            i+=1
            if i == 0:
                continue
            string_list = stg.split(",")
            dict[string_list[0]]=[string_list[1], string_list[2]]
            
        name = ""
        product = ""
        i = -1
        j = last_i + 1
        count = 0
        for r in reader:
            print(r)
            stg = r[0]
            print('\n')
            i+=1
            if i == 0:
                continue
            string_list = stg.split(",")
            if i == 1:
                name = string_list[0]
                product = string_list[1]
                count = 1
                continue
            elif name == string_list[0] and product == string_list[1]:
                count += 1
            else:
                #dict[name][0] = count
                orgin = str(dict[name][0])
                dest = str(dict[name][1])
                print("here\n")
                print(j, j, orgin, count,type(orgin), dest, type(dest), name, product)
                cur.execute("insert into shipment VALUES (?, ?, ?, ?, ?)", (j, j, count, orgin, dest))
                con.commit()
                print("uuuuuuuuu")
                cur.execute("insert into product VALUES (?, ?)", (j, product))
                con.commit()
          #      print("here\n")
                name = string_list[0]
                product = string_list[1]
                count = 1
                j += 1
except:
    print("Cannot read files")

    
