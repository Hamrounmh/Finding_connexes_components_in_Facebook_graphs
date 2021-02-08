#!/usr/bin/env/python
import sys

currentKey = None
tmplist = []
conteur = 0


def CcfIteration(key, values):
    cpt = 0
    minVal = key
    valueList = []
    elements = values
    for elm in elements:
        if elm < minVal:
            minVal = elm
        valueList.append(elm)
    if minVal < key:
        print('%s\t%s' % (key, minVal))
        for val in valueList:
            if minVal != val:
                cpt += 1
                print('%s\t%s' % (val, minVal))
    return cpt


for line in sys.stdin:
    line = line.strip()
    currentLine = line.split('\t')
    key = currentLine[0]
    value = currentLine[1]

    if currentKey is None:
        currentKey = key
        tmplist.append(value)
    else:
        if currentKey == key:
            tmplist.append(value)
        else:
            conteur += CcfIteration(currentKey, tmplist)
            currentKey = key
            tmplist = [value]
if key == currentKey:
    conteur += CcfIteration(currentKey, tmplist)