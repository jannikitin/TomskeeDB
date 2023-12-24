import tomskeedb as tsk
import numpy as np
import csv
if __name__ == '__main__':
    table = tsk.Table({'id':[1,2,3,4,5,6,7,8],
                       'name':['Albert', 'Bob', 'Claire', 'Kyle', 'Mon', 'William','Hovard','Kevin'],
                       'score':[19.2, 90, 92.3,55.67,93.4,1.22,99.3,67.4]})
    table.select()
