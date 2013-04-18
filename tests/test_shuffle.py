import os
from unittest import TestCase
from nose.tools import eq_
import tempfile
import shutil
import gzip

from leisure import shuffle, disco
from leisure.path import makedirs

def cat(fname, content):
  open(fname,'w').write(content)


class TestShuffle(TestCase):

  def setUp(self):

    self.data_root = tempfile.mkdtemp()

    self.job_name = "Job@123"
    self.host = "localhost"
    self.job_home = disco.job_home(self.job_name, os.path.join(self.data_root, self.host))
    self.job_url = disco.job_url(self.host, self.job_name)

    makedirs(self.job_home)
 
    self.part_info = self.make_part_info(self.job_home)
     
 

  def tearDown(self):
    shutil.rmtree(self.data_root)

  def make_part_info(self, job_home):
    part_dir = "partitions-{}".format(disco.timestamp())
    part_path = os.path.join(
      job_home,
      part_dir
    )
    makedirs(part_path)

    part_url = os.path.join("disco://localhost", self.job_url, part_dir)

    return (
      part_path,
      part_url
    )

  def mk_output_file(self, name, content, job_home=None):
    if job_home is None:
      job_home = self.job_home

    path = os.path.join(job_home, name)
    cat(path, content)
    return path


  def mk_task_results(self, task_name, mode='map', host="localhost"):
    """
    Creates a file suitable for using as task results and return it's url
    """

    job_home = disco.job_home(self.job_name, os.path.join(self.data_root, host))

    self.mk_output_file('{}-0'.format(mode), 
      'line1\n'
      'line2\n',
      job_home=job_home
    )

    self.mk_output_file('{}-1'.format(mode), 
      'line1\n'
      'line2\n',
      job_home=job_home
    )

    self.mk_output_file('{}-2'.format(mode), 
      'line1\n'
      'line2\n',
      job_home=job_home
    )


  

    job_url = disco.job_url(host, self.job_name)

    makedirs(job_home)
    task_result_path = os.path.join(job_home, task_name)

    cat(task_result_path, 
      (
        "0  part://{host}/{job_url}/{mode}-0\n"
        "1  part://{host}/{job_url}/{mode}-1\n"
        "0  part://{host}/{job_url}/{mode}-2\n"
      ).format(job_url = job_url, host=host, mode=mode)
    )

    return os.path.join("disco://", host, job_url, task_name)


  def test_write_index(self):
    index = [
      "line1\n",
      "line2\n"
    ]

    filename = os.path.join(self.data_root, "blah")
    shuffle.write_index(filename, index)

    read_lines = gzip.GzipFile(filename).readlines()
    self.assertSequenceEqual(index, read_lines)

  def test_process_url_non_local(self): 

    eq_(
      '0 tag://blah\n',
      shuffle.process_url(
        ("0", "tag://blah"), 
        self.data_root,
        self.part_info
      )
    )

  def test_process_url_local(self):
    self.mk_output_file('map-0', 
      'line1\n'
      'line2\n'
    )

    self.mk_output_file('map-1', 
      'line3\n'
      'line4\n'
    )
 
    part_path,part_url = self.part_info
    part_dir  = os.path.basename(part_path)
    
    eq_(
      '0 disco://localhost/{}/{}/part-0\n'.format(self.job_url, part_dir),
      shuffle.process_url(
        ("0", "part://localhost/{}/map-0".format(self.job_url)), 
        self.data_root,
        self.part_info
      )
    )

    eq_(
      open(os.path.join(part_path, "part-0")).read(),
      'line1\n'
      'line2\n'
    )

    eq_(
      '0 disco://localhost/{}/{}/part-0\n'.format(self.job_url, part_dir),
      shuffle.process_url(
        ("0", "part://localhost/{}/map-1".format(self.job_url)), 
        self.data_root,
        self.part_info
      )
    )

    eq_(
      open(os.path.join(part_path, "part-0")).read(),
      'line1\n'
      'line2\n'
      'line3\n'
      'line4\n'
    )

  def test_process_task(self):
    task_result_url = self.mk_task_results('task-1')
    part_files = list(shuffle.process_task(
      task_result_url, 
      self.data_root, self.part_info
    ))

    part_url = self.part_info[1]

    expected = [ 
      s.format(part_url=part_url) for s in [
        "0 {part_url}/part-0\n",
        "1 {part_url}/part-1\n",
        "0 {part_url}/part-0\n"
      ]
    ] 

    self.assertSequenceEqual(
      expected,
      part_files
    )

  def test_merged_index(self):
    dir_urls = [self.mk_task_results('task-1')]

    m_index = shuffle.merged_index(dir_urls, self.data_root, self.part_info)

    part_url = self.part_info[1]

    expected = [ 
      s.format(part_url=part_url) for s in [
        "0 {part_url}/part-0\n",
        "1 {part_url}/part-1\n",
      ]
    ] 

    self.assertSequenceEqual(
      set(expected),
      m_index
    )

  def test_combine_tasks(self):
    task_results =[
      [
        "node1", 
        self.mk_task_results('task-1', "node1",)
      ],

      ["node2", self.mk_task_results('task-1', "node2")],
      ["node1", self.mk_task_results('task-2', "node1")]
    ]
 
    indexes = list(shuffle.combine_tasks(
      data_root=self.data_root,
      job=self.job_name, 
      mode="map", 
      task_results=task_results
    ))


