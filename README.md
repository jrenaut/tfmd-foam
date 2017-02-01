# tfmd-foam
Pulling together some data for Trash Free Maryland on foam container usage in DC restaurants

Trash Free Maryland wanted to see if there was a relation between the neighborhood a restaurant was in and whether or not they used foam containers. They did a survey of DC restaurants to find out which used foam.

This analysis finds the census tract for each restaurant, then looks at race and income for that tract.

The restaurant address data is not public, so you can't recreate this, but I'm going to see if I can scrub enough to both keep it private and make it interesting to anyone.

Includes a Dockerfile and docker-compose. It's a Postgres database container - I just SSHed and ran a bit of Python to put the data together.
