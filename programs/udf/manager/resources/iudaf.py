#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np

class IUDAF():
    def addBatchSinglePlace(self, output, inputs):
        nargs = len(inputs)
        size = len(inputs[0])
        for i in range(size):
            ins = [inputs[j][i] for j in range(nargs)]
            output[0] = self.add(*ins, output[0])

    def addBatch(self, output, inputs, rowsMap):
        nargs = len(inputs)
        size = len(inputs[0])
        for i in range(size):
            ins = [inputs[j][i] for j in range(nargs)]
            output[rowsMap[i]] = self.add(*ins, output[rowsMap[i]])

    def mergeDataBatch(self, output, state, rowsMap):
        for i in range(len(output)):
            output[i] = state[0][i]
        for i in range(len(rowsMap)):
            output[rowsMap[i]] = self.merge(output[rowsMap[i]], state[1][i])

    def mergeBatch(self, output, state):
        for i in range(len(output)):
            output[i] = self.merge(state[0][i], state[1][i])


    def add(self, *args):
        pass

    def merge(self, *args):
        pass