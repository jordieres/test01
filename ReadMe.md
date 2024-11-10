# ShortTerm scenario from the DynReAct project

The repository comes with a Short Term sample scenario, defined in this subfolder.
It does include several agent prototypes, such as

* **log.py**: Log Agent, in charge for recording all the messages

* **resource.py**: Resource Agent (plants), in charge of handling the auction's bids 
                   considering the different setup status parameters.

* **material.py**: Material Agent (coils), in charge of representing the status
                   of the material and its willingness to offer for a slot in the
                   next auction.

* **ShortTermPlanning.py**: Main script in charge of simulating several contexts for
                   the auction. 
		   The scenario requires to have a Kafka broker avaliable to all the
                   agent instances.
                   To make possible to reset the broker queues a tool has been created
                   and named **clean_kafka.py**.


## Prerequisites

Python versions >= 3.10 and <= 3.12 should be supported. 

### Install dependencies

First, create a virtual environment for the project. Navigate to the folder, and execute

```commandline
python -m venv --upgrade-deps venv
```

You can run the setup:

```commandline
pip install -r requirements_local.txt 
```

## Run

Different use cases can be simulated by testing the following commands:

```commandline
python3 ShortTermPlanning.py -v 3  -b . -rw 10 -cw 30 -aw 50 -bw 15 -ew 10 -r 14 -n 1
python3 ShortTermPlanning.py -v 3  -b . -rw 10 -cw 30 -aw 50 -bw 15 -ew 10 -r 14 -n 2
python3 ShortTermPlanning.py -v 3 -b . -rw 10 -cw 30 -aw 200 -bw 15 -ew 10 -r 14 15 -n 1
python3 ShortTermPlanning.py -v 3 -b . -rw 10 -cw 30 -aw 200 -bw 15 -ew 10 -r 14 15 -n 4
python3 ShortTermPlanning.py -v 3 -b . -rw 100 -cw 300 -aw 1200 -bw 45 -ew 100 -r 14 15
```

Be sure that the live kafka broker has been properly configured in the file *config.cnf*.

