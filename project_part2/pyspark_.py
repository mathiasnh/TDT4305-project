
def polarity(s):
    split = s.split()
    lst = list(filter(lambda x: x not in stopwords, split))
    return sum(map(lambda x: int(afinn_dct.get(x, 0)), lst))