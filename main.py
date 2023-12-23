import TomskeeDB
import numpy as np


if __name__ == '__main__':
    cols = [x for x in 'ABCDEFG']
    data = np.random.randint(1, 10, size=(10, len('ABCDEFG')))
    test = TomskeeDB.Table(data, cols, axis=1)
    test.print()
    print(test.shape)
