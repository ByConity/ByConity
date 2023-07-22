#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np

class IUDF():
    def entry(self, output, inputs):
        nargs = len(inputs)

        it = np.nditer([output] + inputs,
                ['refs_ok', 'buffered'], [['writeonly']] + [['readonly']]*nargs)

        with it:
            for out, *ins in it:
                out[...] = self.process(*[x.item() for x in ins])

    def process(self, *args):
        pass
