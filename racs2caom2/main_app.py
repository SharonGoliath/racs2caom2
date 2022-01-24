# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

"""
This module implements the ObsBlueprint mapping, as well as the workflow 
entry point that executes the workflow.
"""

from os.path import basename
from math import sqrt
from urllib.parse import urlparse
from caom2 import ProductType
from caom2pipe import astro_composable as ac
from caom2pipe import caom_composable as cc
from caom2pipe import manage_composable as mc


__all__ = [
    'RACSName',
    'COLLECTION',
    'APPLICATION', 
]


APPLICATION = 'racs2caom2'
CIRADA_SCHEME = 'cadc'
COLLECTION = 'RACS'
SCHEME = 'casda'


class RACSName(mc.StorageName):
    """Isolate the relationship between the observation id and the
    file names.

    Isolate the zipped/unzipped nature of the file names.

    While tempting, it's not possible to recreate URLs from file names,
    because some of the URLs are from the QA_REJECTED directories, hence
    the absence of that functionality in this class.
    """

    def __init__(
            self,
            entry=None,
    ):
        self._collection = COLLECTION
        self._entry = entry.replace('.header', '')
        self._vos_url = None
        temp = urlparse(entry.replace('.header', ''))
        if temp.scheme == '':
            self._url = None
            self._file_name = basename(entry.replace('.header', ''))
        else:
            if temp.scheme.startswith('http') or temp.scheme.startswith('vos'):
                self._url = entry.replace('.header', '')
                self._file_name = basename(temp.path)
                self._vos_url = entry.replace('.header', '')
            else:
                # it's an Artifact URI
                self._url = None
                self._file_name = temp.path.split('/')[-1]
        self._obs_id = RACSName.get_obs_id_from_file_name(self._file_name)
        self._product_id = RACSName.get_product_id_from_file_name(
            self._file_name
        )
        self._file_id = RACSName.remove_extensions(self._file_name)
        self._version = RACSName.get_version(self._file_name)
        self._scheme = SCHEME
        self._source_names = [entry]
        self._destination_uris = [self.file_uri]

    def __str__(self):
        return (
            f'\n'
            f'      obs_id: {self.obs_id}\n'
            f'     file_id: {self.file_id}\n'
            f'   file_name: {self.file_name}\n'
            f'source_names: {self.source_names}\n'
            f'    file_uri: {self.file_uri}\n'
            f'         url: {self.url}\n'
        )

    @property
    def file_id(self):
        return self._file_id

    @property
    def file_uri(self):
        return self._get_uri(self._file_name, SCHEME)

    @property
    def file_name(self):
        return self._file_name

    @property
    def prev(self):
        return f'{self._file_id}_prev.jpg'

    @property
    def prev_uri(self):
        return self._get_uri(self.prev, CIRADA_SCHEME)

    @property
    def product_id(self):
        return RACSName.get_product_id_from_file_name(self.file_name)

    @property
    def scheme(self):
        return self._scheme

    @property
    def source_names(self):
        return self._source_names

    @property
    def thumb(self):
        return f'{self._file_id}_prev_256.jpg'

    @property
    def thumb_uri(self):
        return self._get_uri(self.thumb, CIRADA_SCHEME)

    def is_valid(self):
        return True

    @property
    def version(self):
        return self._version

    def _get_uri(self, file_name, scheme):
        return cc.build_artifact_uri(file_name, self._collection, scheme)

    @staticmethod
    def get_obs_id_from_file_name(file_name):
        """The obs id is made of the VLASS epoch, tile name, and image centre
        from the file name.
        """
        bits = file_name.split('_')
        obs_id = f'{bits[0]}_{bits[1]}'
        return obs_id

    @staticmethod
    def get_product_id_from_file_name(file_name):
        bits = file_name.split('_')
        return bitsi[2]

    @staticmethod
    def get_version(file_name):
        bits = file_name.split('-')[1]
        return bits.split("_")[0]

    @staticmethod
    def remove_extensions(file_name):
        return file_name.replace('.fits', '').replace('.header', '')


class RACSMapping(cc.TelescopeMapping):
    def __init__(self, storage_name, headers):
        super().__init__(storage_name, headers)

    def accumulate_bp(self, bp, application=None):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp, APPLICATION)
        bp.configure_position_axes((1, 2))
        bp.configure_energy_axis(3)
        bp.configure_polarization_axis(4)

        # observation level
        bp.set('Observation.type', 'OBJECT')

        # over-ride use of value from default keyword 'DATE'
        bp.set('Observation.metaRelease', '2023-01-01')

        # Clare Chandler via JJK - 21-08-18
        bp.set('Observation.instrument.name', 'ASKAP')
        # From JJK - 27-08-18 - slack
        bp.set('Observation.proposal.title', 'RACS')
        bp.set('Observation.proposal.project', 'RACS')
        bp.set('Observation.proposal.id', 'get_proposal_id(uri)')

        # plane level
        bp.set('Plane.calibrationLevel', '2')
        bp.set('Plane.dataProductType', 'cube')

        # Clare Chandler via slack - 28-08-18
        bp.clear('Plane.provenance.name')
        bp.add_fits_attribute('Plane.provenance.name', 'ORIGIN')
        bp.set('Plane.provenance.producer', 'CSIRO')
        # From JJK - 27-08-18 - slack
        bp.set('Plane.provenance.project', 'WALLABY')

        bp.clear('Plane.metaRelease')
        bp.set('Plane.metaRelease', '2023-01-01')
        bp.clear('Plane.dataRelease')
        bp.set('Plane.dataRelease', '2023-01-01')

        # artifact level
        bp.clear('Artifact.productType')
        bp.set('Artifact.productType', 'get_product_type(uri)')
        bp.set('Artifact.releaseType', 'data')

        # chunk level
        bp.clear('Chunk.position.axis.function.cd11')
        bp.clear('Chunk.position.axis.function.cd22')
        bp.add_fits_attribute('Chunk.position.axis.function.cd11', 'CDELT1')
        bp.set('Chunk.position.axis.function.cd12', 0.0)
        bp.set('Chunk.position.axis.function.cd21', 0.0)
        bp.add_fits_attribute('Chunk.position.axis.function.cd22', 'CDELT2')

        # Clare Chandler via JJK - 21-08-18
        bp.set('Chunk.energy.bandpassName', 'S-band')
        bp.add_fits_attribute('Chunk.energy.restfrq', 'RESTFREQ')
        self._logger.debug('End accumulate_wcs')
        self._logger.debug('Done accumulate_bp.')

    def get_position_resolution(self, ext):
        bmaj = self._headers[ext]['BMAJ']
        bmin = self._headers[ext]['BMIN']
        # From
        # https://open-confluence.nrao.edu/pages/viewpage.action?pageId=13697486
        # Clare Chandler via JJK - 21-08-18
        return 3600.0 * sqrt(bmaj * bmin)

    def get_product_type(self, ext):
        if '.rms.' in self._uri:
            return ProductType.NOISE
        else:
            return ProductType.SCIENCE

    def get_proposal_id(self, ext):
        caom_name = mc.CaomName(self._uri)
        bits = caom_name.file_name.split('.')
        return f'{bits[0]}.{bits[1]}'

    def get_time_refcoord_value(self, ext):
        dateobs = self._headers[ext].get('DATE-OBS')
        if dateobs is not None:
            result = ac.get_datetime(dateobs)
            if result is not None:
                return result.mjd
            else:
                return None

    def _update_artifact(self, artifact, caom_repo_client):
        if artifact.uri.startswith('vos:cirada'):
            old_uri = artifact.uri
            artifact.uri = old_uri.replace(
                'vos:cirada', 'vos://cadc.nrc.ca~vault/cirada'
            )
            self._logger.info(f'Change URI from {old_uri} to {artifact.uri}')

    def update(self, observation, file_info, caom_repo_client):
        """Called to fill multiple CAOM model elements and/or attributes
        (an n:n relationship between TDM attributes and CAOM attributes).
        """
        super().update(observation, file_info, caom_repo_client)
        return observation
