from pyspark.sql import types as T


def string_to_type(s):
    tname = s.lower()
    if tname == "int":
        return T.IntegerType()
    elif tname in ["long", "timestamp"]:
        return T.LongType()
    elif tname == "double":
        return T.DoubleType()
    else:
        return T.StringType()


def random_cell(field_type, mode):
    r = re.compile(r"^.*RAND(\(([0-9]+)-([0-9]+)\))?.*$")

    (_, l, u) = r.match(mode).groups()

    lower = int(l) if l else 0
    upper = int(u) if u else 2147483647

    t = field_type.lower()
    if t in ["int", "long", "timestamp"]:
        return random.randint(lower, upper)
    elif t == "double":
        return random.random()
    else:
        return ''.join(random.choices(string.ascii_lowercase, k=24))


def parse_ts(s):
    t = time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S"))
    return int(t)


def process_cells(cols, cells):
    data = list(zip(cols, cells))
    for ((_, ftype), cell) in data:

        if "%RAND" in cell:
            yield random_cell(ftype, cell)
        elif ftype == "timestamp":
            yield parse_ts(cell)
        else:
            yield cell


def table_to_spark(spark, table):
    cols = [h.split(':') for h in table.headings]
    if len([c for c in cols if len(c) != 2]) > 0:
        raise ValueError("You must specify name AND data type for columns like this 'my_field:string'")

    schema = T.StructType([T.StructField(name + "_str", T.StringType(), False) for (name, _) in cols])
    rows = [list(process_cells(cols, row.cells)) for row in table]
    df = spark.createDataFrame(rows, schema=schema)
    for (name, field_type) in cols:
        df = (
            df
                .withColumn(name, df[name + "_str"].cast(string_to_type(field_type)))
                .drop(name + "_str")
        )
    return df

