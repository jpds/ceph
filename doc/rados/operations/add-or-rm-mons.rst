==========================
 Adding/Removing Monitors
==========================

When you have a cluster up and running, you may add or remove monitors
from the cluster at runtime.

Adding Monitors
===============

Ceph monitors are light-weight processes that maintain a master copy of the 
cluster map. You can run a cluster with 1 monitor. We recommend at least 3 
monitors for a production cluster. Ceph monitors use PAXOS to establish 
consensus about the master cluster map, which requires a majority of
monitors running to establish a quorum for consensus about the cluster map
(e.g., 1; 3 out of 5; 4 out of 6; etc.).

Since monitors are light-weight, it is possible to run them on the same 
host as an OSD; however, we recommend running them on separate hosts. 

.. important:: A *majority* of monitors in your cluster must be able to 
   reach each other in order to establish a quorum.

Deploy your Hardware
--------------------

If you are adding a new host when adding a new monitor,  see `Hardware
Recommendations`_ for details on minimum recommendations for monitor hardware.
To add a monitor host to your cluster, first make sure you have an up-to-date
version of Linux installed (typically Ubuntu 12.04 precise). 

Add your monitor host to a rack in your cluster, connect it to the network
and ensure that it has network connectivity.

.. _Hardware Recommendations: ../../../start/hardware-recommendations

Install the Required Software
-----------------------------

For manually deployed clusters, you must install Ceph packages
manually. See `Installing Debian/Ubuntu Packages`_ for details.
You should configure SSH to a user with password-less authentication
and root permissions.

.. _Installing Debian/Ubuntu Packages: ../../../install/debian



.. _Adding a Monitor (Manual):

Adding a Monitor (Manual)
-------------------------

This procedure creates a ``ceph-mon`` data directory, retrieves the monitor map
and monitor keyring, and adds a ``ceph-mon`` daemon to your cluster.  If
this results in only two monitor daemons, you may add more monitors by
repeating this procedure until you have a sufficient number of ``ceph-mon`` 
daemons to achieve a quorum.

At this point you should define your monitor's id.  Traditionally, monitors 
have been named with single letters (``a``, ``b``, ``c``, ...), but you are 
free to define the id as you see fit.  For the purpose of this document, 
please take into account that ``{mon-id}`` should be the id you chose, 
without the ``mon.`` prefix (i.e., ``{mon-id}`` should be the ``a`` 
on ``mon.a``).

#. Create the default directory on your new monitor. :: 

	ssh {new-mon-host}
	sudo mkdir /var/lib/ceph/mon/ceph-{mon-id}

#. Create a temporary directory ``{tmp}`` to keep the files needed during 
   this process. This directory should be different from monitor's default 
   directory created in the previous step, and can be removed after all the 
   steps are taken. :: 

	mkdir {tmp}

#. Retrieve the keyring for your monitors, where ``{tmp}`` is the path to 
   the retrieved keyring, and ``{filename}`` is the name of the file containing
   the retrieved monitor key. :: 

	ceph auth get mon. -o {tmp}/{filename}

#. Retrieve the monitor map, where ``{tmp}`` is the path to 
   the retrieved monitor map, and ``{filename}`` is the name of the file 
   containing the retrieved monitor monitor map. :: 

	ceph mon getmap -o {tmp}/{filename}

#. Prepare the monitor's data directory created in the first step. You must 
   specify the path to the monitor map so that you can retrieve the 
   information about a quorum of monitors and their ``fsid``. You must also 
   specify a path to the monitor keyring:: 

	sudo ceph-mon -i {mon-id} --mkfs --monmap {tmp}/{filename} --keyring {tmp}/{filename}
	

#. Add a ``[mon.{mon-id}]`` entry for your new monitor in your ``ceph.conf`` file. ::

	[mon.c]
		host = new-mon-host
		addr = ip-addr:6789

#. Add the new monitor to the list of monitors for you cluster (runtime). This enables 
   other nodes to use this monitor during their initial startup. ::

	ceph mon add <mon-id> <ip>[:<port>]

#. Start the new monitor and it will automatically join the cluster.
   The daemon needs to know which address to bind to, either via
   ``--public-addr {ip:port}`` or by setting ``mon addr`` in the
   appropriate section of ``ceph.conf``.  For example::

	ceph-mon -i {mon-id} --public-addr {ip:port}


Removing Monitors
=================

When you remove monitors from a cluster, consider that Ceph monitors use 
PAXOS to establish consensus about the master cluster map. You must have 
a sufficient number of monitors to establish a quorum for consensus about 
the cluster map.

.. _Removing a Monitor (Manual):

Removing a Monitor (Manual)
---------------------------

This procedure removes a ``ceph-mon`` daemon from your cluster.   If this
procedure results in only two monitor daemons, you may add or remove another
monitor until you have a number of ``ceph-mon`` daemons that can achieve a 
quorum.

#. Stop the monitor. ::

	service ceph -a stop mon.{mon-id}
	
#. Remove the monitor from the cluster. ::

	ceph mon remove {mon-id}
	
#. Remove the monitor entry from ``ceph.conf``. 


Removing Monitors from an Unhealthy Cluster
-------------------------------------------

This procedure removes a ``ceph-mon`` daemon from an unhealhty cluster--i.e., 
a cluster that has placement groups that are persistently not ``active + clean``.


#. Identify a surviving monitor and log in to that host. :: 

	ceph mon dump
	ssh {mon-host}

#. Stop the ``ceph-mon`` daemon and extract a copy of the monap file.  ::

	service ceph stop mon || stop ceph-mon-all
        ceph-mon -i {mon-id} --extract-monmap {map-path}
	# for example,
        ceph-mon -i a --extract-monmap /tmp/monmap

#. Remove the non-surviving monitors. 	For example, if you have three monitors, 
   ``mon.a``, ``mon.b``, and ``mon.c``, where only ``mon.a`` will survive, follow 
   the example below:: 

	monmaptool {map-path} --rm {mon-id}
	# for example,
	monmaptool /tmp/monmap --rm b
	monmaptool /tmp/monmap --rm c
	
#. Inject the surviving map with the removed monitors into the surviving monitors. 
   For example, to inject a map into monitor ``mon.a``, follow the example below:: 

	ceph-mon -i {mon-id} --inject-monmap {map-path}
	# for example,
	ceph-mon -i a --inject-monmap /tmp/monmap


.. _Changing a Monitor's IP address:

Changing a Monitor's IP Address
===============================

.. important:: Existing monitors are not supposed to change their IP addresses.

Monitors are critical components of a Ceph cluster, and they need to maintain a
quorum for the whole system to work properly. To establish a quorum, the
monitors need to discover each other. Ceph has strict requirements for
discovering monitors.

Ceph clients and other Ceph daemons use ``ceph.conf`` to discover monitors.
However, monitors discover each other using the monitor map, not ``ceph.conf``.
For example,  if you refer to `Adding a Monitor (Manual)`_ you will see that you
need to obtain the current monmap for the cluster when creating a new monitor,
as it is one of the required arguments of ``ceph-mon -i {mon-id} --mkfs``. The
following sections explain the consistency requirements for Ceph monitors, and a
few safe ways to change a monitor's IP address.

Consistency Requirements
------------------------

A monitor always refers to the local copy of the monmap  when discovering other
monitors in the cluster.  Using the monmap instead of ``ceph.conf`` avoids
errors that could  break the cluster (e.g., typos in ``ceph.conf`` when
specifying a monitor address or port). Since monitors use monmaps for discovery
and they share monmaps with clients and other Ceph daemons, the monmap provides
monitors with a strict guarantee that their consensus is valid.

Strict consistency also applies to updates to the monmap. As with any other
updates on the monitor, changes to the monmap always run through a distributed
consensus algorithm called `Paxos`_. The monitors must agree on each update to
the monmap, such as adding or removing a monitor, to ensure that each monitor in
the quorum has the same version of the monmap. Updates to the monmap are
incremental so that monitors have the latest agreed upon version, and a set of
previous versions, allowing a monitor that has an older version of the monmap to
catch up with the current state of the cluster.

If monitors discovered each other through the Ceph configuration file instead of
through the monmap, it would introduce additional risks because the Ceph
configuration files aren't updated and distributed automatically. Monitors
might inadvertantly use an older ``ceph.conf`` file, fail to recognize a
monitor, fall out of a quorum, or develop a situation where `Paxos`_ isn't able
to determine the current state of the system accurately. Consequently,  making
changes to an existing monitor's IP address must be done with  great care.

.. _Paxos: http://en.wikipedia.org/wiki/Paxos_(computer_science)


Changing a Monitor's IP address (The Right Way)
-----------------------------------------------

Changing a monitor's IP address in ``ceph.conf`` only is not sufficient to
ensure that other monitors in the cluster will receive the update.  To change a
monitor's IP address, you must add a new monitor with the IP  address you want
to use (as described in `Adding a Monitor (Manual)`_),  ensure that the new
monitor successfully joins the  quorum; then, remove the monitor that uses the
old IP address. Then, update the ``ceph.conf`` file to ensure that clients and
other daemons know the IP address of the new monitor.

For example, lets assume there are three monitors in place, such as :: 

	[mon.a]
		host = host01
		addr = 10.0.0.1:6789
	[mon.b]
		host = host02
		addr = 10.0.0.2:6789
	[mon.c]
		host = host03
		addr = 10.0.0.3:6789

To change ``mon.c`` to ``host04`` with the IP address  ``10.0.0.4``, follow the
steps in `Adding a Monitor (Manual)`_ by adding a  new monitor ``mon.d``. Ensure
that ``mon.d`` is  running before removing ``mon.c``, or it will break the
quorum. Remove ``mon.c`` as described on  `Removing a Monitor (Manual)`_. Moving
all three  monitors would thus require repeating this process as many times as
needed.

Changing a Monitor's IP address (The Messy Way)
-----------------------------------------------

There may come a time when the monitors must be moved to a different network,  a
different part of the datacenter or a different datacenter altogether. While  it
is possible to do it, the process becomes a bit more hazardous.

In such a case, the solution is to generate a new monmap with updated IP
addresses for all the monitors in the cluster, and inject the new map on each
individual monitor.  This is not the most user-friendly approach, but we do not
expect this to be something that needs to be done every other week.  As it is
clearly stated on the top of this section, monitors are not supposed to change
IP addresses.

Using the previous monitor configuration as an example, assume you want to move
all the  monitors from the ``10.0.0.x`` range to ``10.1.0.x``, and these
networks  are unable to communicate.  Use the following procedure:

#. Retrieve the monitor map, where ``{tmp}`` is the path to 
   the retrieved monitor map, and ``{filename}`` is the name of the file 
   containing the retrieved monitor monitor map. :: 

	ceph mon getmap -o {tmp}/{filename}

#. The following example demonstrates the contents of the monmap. ::

	$ monmaptool --print {tmp}/{filename}
	
	monmaptool: monmap file {tmp}/{filename}
	epoch 1
	fsid 224e376d-c5fe-4504-96bb-ea6332a19e61
	last_changed 2012-12-17 02:46:41.591248
	created 2012-12-17 02:46:41.591248
	0: 10.0.0.1:6789/0 mon.a
	1: 10.0.0.2:6789/0 mon.b
	2: 10.0.0.3:6789/0 mon.c

#. Remove the existing monitors. ::

	$ monmaptool --rm a --rm b --rm c {tmp}/{filename}
	
	monmaptool: monmap file {tmp}/{filename}
	monmaptool: removing a
	monmaptool: removing b
	monmaptool: removing c
	monmaptool: writing epoch 1 to {tmp}/{filename} (0 monitors)

#. Add the new monitor locations. ::

	$ monmaptool --add a 10.1.0.1:6789 --add b 10.1.0.2:6789 --add c 10.1.0.3:6789 {tmp}/{filename}
	
	monmaptool: monmap file {tmp}/{filename}
	monmaptool: writing epoch 1 to {tmp}/{filename} (3 monitors)

#. Check new contents. ::

	$ monmaptool --print {tmp}/{filename}
	
	monmaptool: monmap file {tmp}/{filename}
	epoch 1
	fsid 224e376d-c5fe-4504-96bb-ea6332a19e61
	last_changed 2012-12-17 02:46:41.591248
	created 2012-12-17 02:46:41.591248
	0: 10.1.0.1:6789/0 mon.a
	1: 10.1.0.2:6789/0 mon.b
	2: 10.1.0.3:6789/0 mon.c

At this point, we assume the monitors (and stores) are installed at the new
location. The next step is to propagate the modified monmap to the new 
monitors, and inject the modified monmap into each new monitor.

#. First, make sure to stop all your monitors.  Injection must be done while 
   the daemon is not running.

#. Inject the monmap. ::

	ceph-mon -i {mon-id} --inject-monmap {tmp}/{filename}

#. Restart the monitors.

After this step, migration to the new location is complete and 
the monitors should operate successfully.
