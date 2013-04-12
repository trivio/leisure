Leisure Suit - Debug and test disco jobs without a cluster
==============



Installation
------------


We reccomend using pip and virtualenv to install.

```
$ virtualenv leisure
$ cd leisure
$ . ./bin/activate
$ pip install -e git+git@github.com:trivio/leisure.git
$ pip install -r src/leisure/requirements.txt 

```

Leisure needs the python runtime for disco to work correctly. To make 
installation as easy as possible we've forked a copy of the current 
disco repo from http://github.com/discoproject.disco.git and 
moved it's setup.py to the root folder to make pip happy. 
Hopefully in the future maintaing this fork will not be neccesary.

Usage
-----
To use leisure simply point it at python script that submits disco jobs.

For example:: 

Using the quintessential word counting example. Create a file called
`word_count.py` with the following content.


```python
from disco.core import Job, result_iterator

def map(line, params):
    for word in line.split():
        yield word, 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

if __name__ == '__main__':
    print "runnning job"
    job = Job().run(input=["http://discoproject.org/media/text/chekhov.txt"],
                    map=map,
                    reduce=reduce)
    for word, count in result_iterator(job.wait(show=True)):
        print(word, count)
``` 

Now execute the script using leisure

```
$ leisure word_count.py
...
```

How it works
-----------

Leisure works by first monkey patching the disco client. It intercepts all
network calls  and excetes them on a  loccally running worker as
specified by your job via the disco worker protocol.

Todo
-----

This is a rabidly evolving work in progress. At current time it only works 
for the mapping phase but we hope to address it shortly.

Our plans are to

* Implment local shuffle and reduce
* Implment mock DDFS
* Support worker debugging
* Remote submission
* Local replay of a failed disco job from a real cluster


Getting Involved
----------------

Contributions are welcome! Feel free to submit patches, bug reports 
and feature requests to our repo at http://github.com/trivio/leisure.

And discuss the project on https://groups.google.com/forum/?fromgroups#!forum/disco-dev



