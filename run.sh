#!/bin/bash

ROOT=/home/devciheul
#ROOT=/Users/winnuayi/Projects/dev

source $ROOT/virtualenv/axes/bin/activate

cd $ROOT/backend/reconcile-aj

$ROOT/virtualenv/axes/bin/python $ROOT/backend/reconcile-aj/reconcile.py
