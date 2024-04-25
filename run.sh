#!/bin/bash
cd scheduler
nohup ./scheduler &
cd ..

cd trigger
nohup ./trigger &
cd ..

cd executor
nohup ./executor &
cd ..
