# -*- coding: utf-8 -*-
"""
python client for influxdb
"""
import json
import socket
import requests
session = requests.Session()


class InfluxDBClientError(Exception):
    "Raised when an error occures in the Request"
    def __init__(self, message, code):
        self.message = message
        self.code = code

    def __str__(self):
        return "{}: {}".format(self.code, self.message)


class InfluxDBClient(object):
    """
    InfluxDB Client
    """

    def __init__(self,
                 host='localhost',
                 port=8086,
                 username='root',
                 password='root',
                 database=None,
                 ssl=False,
                 verify_ssl=False,
                 timeout=None,
                 use_udp=False,
                 udp_port=4444):
        """
        Initialize client
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.timeout = timeout

        self.verify_ssl = verify_ssl

        self.use_udp = use_udp
        self.udp_port = udp_port
        if use_udp:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.scheme = "http"

        if ssl is True:
            self.scheme = "https"

        self._baseurl = "{0}://{1}:{2}".format(
            self.scheme,
            self.host,
            self.port)

        self._headers = {
            'Content-type': 'application/json',
            'Accept': 'text/plain'}

    def request(self, url, method='GET', params=None, data=None,
                status_code=200):
        """
        Make a http request to API
        """
        url = "{0}/{1}".format(self._baseurl, url)

        if params is None:
            params = {}

        args = {
            'u': self._username,
            'p': self._password
        }

        args.update(params)

        if data is not None and not isinstance(data, str):
            data = json.dumps(data)

        response = session.request(
            method=method,
            url=url,
            params=args,
            data=data,
            headers=self._headers,
            verify=self._verify_ssl,
            timeout=self._timeout
            )

        if response.status_code != status_code:
            raise InfluxDBClientError(response.content,
                    response.status_code)

        return response

    def send_packet(self, packet):
        data = json.dumps(packet)
        byte = data.encode('utf-8')
        self.udp_socket.sendto(byte, (self._host, self.udp_port))

    def write_points(self, data, batch_size=None, time_precision="s"):
        """
        Write to multiple time series names

        Parameters
        ----------
        batch_size : Optional. Int value to write the points in batches instead
            of all at one time.
            Useful for when doing data dumps from one database to another or
            when doing a massive write operation
        """
        assert time_precision in "smu"

        if batch_size:
            for dataset in data:
                points = dataset["points"]
                for i in xrange(0, len(points), batch_size):
                    batch = [{
                        "name": dataset["name"],
                        "columns": dataset["columns"],
                        "points": points[i:i+n],
                    }]
                    self.write_points(data=batch,
                        time_precision=time_precision)
            return

        url = "db/{0}/series".format(self._database)
        params = {"time_precision": time_precision}

        if self.use_udp:
            self.send_packet(data)
        else:
            self.request(
                url=url,
                method='POST',
                params=params,
                data=data,
                status_code=200
                )

    def query(self, query, time_precision='s', chunked=False):
        """
        Quering data
        """
        assert time_precision in "smu"

        url = "db/{0}/series".format(self._database)

        params = {
            'q': query,
            'time_precision': time_precision,
            'chunked': chunked
        }

        response = self.request(
            url=url,
            method='GET',
            params=params,
            status_code=200
            )

        try:
            res = json.loads(response.content)
        except TypeError:
            # must decode in python 3
            res = json.loads(response.content.decode('utf8'))

        return res

    def create_database(self, database):
        """
        Create a database

        Parameters
        ----------
        database: string
            database name
        """
        url = "db"

        data = {'name': database}

        self.request(
            url=url,
            method='POST',
            data=data,
            status_code=201
            )

    def delete_database(self, database):
        """
        Drop a database

        Parameters
        ----------
        database: string
            database name
        """
        url = "db/{0}".format(database)

        self.request(
            url=url,
            method='DELETE',
            status_code=204
            )

    def get_database_list(self):
        """
        Get the list of databases
        """
        url = "db"

        response = self.request(
            url=url,
            method='GET',
            status_code=200
            )

        return json.loads(response.content)

    def delete_series(self, series):
        """
        Drop a series

        Parameters
        ----------
        series: string
            series name
        """
        url = "db/{0}/series/{1}".format(
            self._database,
            series
            )

        self.request(
            url=url,
            method='DELETE',
            status_code=204
            )

    def get_list_cluster_admins(self):
        """
        Get list of cluster admins
        """
        response = self.request(
            url="cluster_admins",
            method='GET',
            status_code=200
            )

        return response.json()

    def add_cluster_admin(self, new_username, new_password):
        """
        Add cluster admin
        """
        data = {
            'name': new_username,
            'password': new_password
        }

        self.request(
            url="cluster_admins",
            method='POST',
            data=data,
            status_code=200
            )

    def update_cluster_admin_password(self, username, new_password):
        """
        Update cluster admin password
        """
        url = "cluster_admins/{0}".format(username)

        data = {
            'password': new_password
        }

        self.request(
            url=url,
            method='POST',
            data=data,
            status_code=200
            )

    def delete_cluster_admin(self, username):
        """
        Delete cluster admin
        """
        url = "cluster_admins/{0}".format(username)

        self.request(
            url=url,
            method='DELETE',
            status_code=204
            )

    def set_database_admin(self, username):
        """
        Set user as database admin
        """
        return self.alter_database_admin(username, True)

    def unset_database_admin(self, username):
        """
        Unset user as database admin
        """
        return self.alter_database_admin(username, False)

    def alter_database_admin(self, username, is_admin):
        url = "db/{0}/users/{1}".format(self._database, username)

        data = {'admin': is_admin}

        self.request(
            url=url,
            method='POST',
            data=data,
            status_code=200
            )

    def get_database_users(self):
        """
        Get list of database users
        """
        url = "db/{0}/users".format(self._database)

        response = self.request(
            url=url,
            method='GET',
            status_code=200
            )

        return response.json()

    def add_database_user(self, new_username, new_password):
        """
        Add database user
        """
        url = "db/{0}/users".format(self._database)

        data = {
            'name': new_username,
            'password': new_password
        }

        self.request(
            url=url,
            method='POST',
            data=data,
            status_code=200
            )

    def update_database_user_password(self, username, new_password):
        """
        Update password
        """
        url = "db/{0}/users/{1}".format(self._database, username)

        data = {
            'password': new_password
        }

        self.request(
            url=url,
            method='POST',
            data=data,
            status_code=200
            )

        if username == self._username:
            self._password = new_password

    def delete_database_user(self, username):
        """
        Delete database user
        """
        url = "db/{0}/users/{1}".format(self._database, username)

        self.request(
            url=url,
            method='DELETE',
            status_code=200
            )


