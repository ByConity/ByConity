#!/bin/bash

TEST_OUTPUT=${TEST_OUTPUT:-all_test_output} 
WORK_DIR=${WORK_DIR:-.}
rm -rf $TEST_OUTPUT/*
rm -rf $WORK_DIR/*
