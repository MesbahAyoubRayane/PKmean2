import csv

DATASET = list[list]


def read_data_set(path: str, remove_column_names: bool = False, remove_column=None) -> DATASET:
    if remove_column is None:
        remove_column = []
    dataset = []
    with open(path, 'rt') as f:
        for line in csv.reader(f):
            r = []
            for index,elm in enumerate(line):
                if index in remove_column:continue
                try:
                    x = float(elm)
                except:
                    x = elm
                r.append(x)
            dataset.append(r)

    if remove_column_names:
        dataset.pop(0)

    return dataset


def check_data_set_integrity(dataset: DATASET) -> bool:
    if dataset is None or not dataset: return False
    column_types = dataset[0]
    for column_index in range(len(column_types)):
        for i in range(len(dataset)):
            if type(column_types[column_index]) != type(dataset[i][column_index]):
                return False
    return True
