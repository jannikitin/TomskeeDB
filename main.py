import TomskeeDB as tdb

if __name__ == '__main__':
    csv = 'D:\\Projects\\Python\\database\\test_data\\dota_hero_stats.csv'
    table = tdb.TomskeeDB.read_csv(csv, dtypes=['str', 'str', 'int', 'int', 'str', 'str', 'str', 'list'])
    table.get()
