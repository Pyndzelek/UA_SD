# prompts

## planning

- This is my assignment, prepare a step by step plan from scratch to deploy

- Analyze the current project status and plan what is left to do. Read the instruction and the grading sheet carefully and meet all the criteria. If there is a difficult feature to implement, ask me before planning it.

- How much work is left?

- Is implementing threads lock necessary? Read the instruction and determine wheather i should implement locks, eexplain edge cases that may be useful to have locks implemented

## study

- Look at the current state, help me properly structure kafka listener and producer with relevant topic. I really need to take small steps

- Be my teacher: explain each step with its context, I want to learn and be able to defend every line

- Explain this raw frame byte by byte: b'\x02REGISTER#WS1#Park\x03\x14'

- I dont understand why would i need to check if there is just one bite each time i receive a message

- Explain LRC

- Visualize in a real world situation where each part of the software would live, how they communicate and what triggers what, the whole flow

- How to call the action when the monitor asks central if it has anything for it?

- What does the lock really do in our screnario? I know how locks works but I can't run into problems while teesting without it? I don't see the usage of it. What can go wrong without it?

- When there is a leak, when should it be applied in cental? Should the central talk to the sqlite imidieelty or save it in local memory?

## defense and lab setup

- What is the flow when defending the project? Explain how will it look like step by step. I want to be ready when the professor comes

- At the defense I will have to use the lab computers, how will I do that? What will the setup be? I have already tried running kafka with docker locally on the windows lab comuter and it works

## monitor

- Rewrite the monitor to use two threads and get rid of the sleep function - replace it by the timer. The sleep(1) needs to be gone and there has to be one thread for the engine and one for the central.

## central

- Create an index.html admin panel and create a very primitive api to talk to central directly

- I want a simple index.html that visualizes the stations and lets me trigger actions, no endpoints, a very simple mechanism

- Finish the index.html so it is handy and useful, make it even show the logs. I want to see all of the logs

- How to auto register any new watering station. New stations are started on one computer and field operators on another one, and every new station should be registered automatically. That means that there might be endless registered stations? Is that right? Should I instatly call sqlite to register new station?

## field operator

- Add a terminal menu to the field operator, e.g. pressing 1 starts watering

- Implement timeouts to field operator

## watering station

- I want to start a station and observe its logs inside docker

- If you request watering while the engine is down you will wait forever

- Add a second watering station predefined in the docker compose

## debugging

- Something is not right, look at central.py, there must be errors. I cant get it together and i don't see any underlines in vs

- Look for the error [docker build command error]

- Flow and volume show undefined in the panel

## deployment

- There has been an issue with railway, I am unable to create an account, we have to go with aws instead. Check that starting each program in the console aligns with the requirements and provide me with starting commands to start each module

- Give me instructions to set up the ec2 instance

- Set up the github secrets for automatic deploy

- Set up github actions for automatic redeploy on aws

- Create all of the necessary docker files. makee sure starting from terminal accepts flexible parameters

- Create the compose files for the lab computers (one for the watering stations, one for the field operators)

## report

- Draft the lab report

## other

- How do I get rid of the macos .DS_Store cached file with git
