# pull image
docker pull voyz/ibeam

# create a file that contains IBKR account's username and password
vi env.list

# run container process
docker run --env-file env.list -p 5000:5000 voyz/ibeam
