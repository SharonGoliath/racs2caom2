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
from caom2pipe.manage_composable import CaomName, convert_to_days, StorageName


__all__ = ['RACSName']


class RACSName(StorageName):
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
        self._entry = entry.replace('.header', '')
        self._vos_url = None
        temp = urlparse(entry.replace('.header', ''))
        if temp.scheme == '':
            self._file_name = basename(entry.replace('.header', ''))
        else:
            if temp.scheme.startswith('http') or temp.scheme.startswith('vos'):
                self._file_name = basename(temp.path)
                self._vos_url = entry.replace('.header', '')
            else:
                # it's an Artifact URI
                self._file_name = temp.path.split('/')[-1]
        super().__init__(file_name=self._file_name, source_names=[entry])
        self.set_version()

    @property
    def version(self):
        return self._version

    def set_file_id(self):
        self._file_id = RACSName.remove_extensions(self._file_name)

    def set_obs_id(self, **kwargs):
        bits = self._file_name.split('.')
        if len(bits) == 2:
            self._obs_id = self._file_id
        else:
            self._obs_id = f'{bits[0]}.{bits[2]}.{bits[3]}.{bits[4]}'

    def set_product_id(self, **kwargs):
        self._product_id = self._file_id

    def set_version(self):
        bits = self._file_name.split('.')
        if len(bits) == 2:
            # DR1 file names do not have a version number
            self._version = None
        else:
            self._version = bits[5]


class RACSMapping(cc.TelescopeMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)
        bp.configure_position_axes((1, 2))
        bp.configure_energy_axis(3)
        bp.configure_polarization_axis(4)

        # observation level
        bp.set('Observation.type', 'OBJECT')

        # over-ride use of value from default keyword 'DATE'
        bp.set('Observation.metaRelease', '2022-01-01')

        bp.set('Observation.instrument.name', 'ASKAP')
        bp.set('Observation.proposal.title', 'RACS')
        bp.set('Observation.proposal.project', 'RACS')
        bp.set('Observation.proposal.id', 'get_proposal_id()')

        # plane level
        bp.set('Plane.calibrationLevel', '2')
        bp.set('Plane.dataProductType', 'image')

        bp.clear('Plane.provenance.name')
        bp.add_attribute('Plane.provenance.name', 'ORIGIN')
        bp.set('Plane.provenance.producer', 'CSIRO')
        bp.set('Plane.provenance.project', 'RACS')

        bp.clear('Plane.metaRelease')
        bp.set('Plane.metaRelease', '2022-01-01')
        bp.clear('Plane.dataRelease')
        bp.set('Plane.dataRelease', '2022-01-01')

        # artifact level
        bp.clear('Artifact.productType')
        bp.set('Artifact.productType', 'get_product_type(uri)')
        bp.set('Artifact.releaseType', 'data')

        # chunk level
        bp.clear('Chunk.position.axis.function.cd11')
        bp.clear('Chunk.position.axis.function.cd22')
        bp.add_attribute('Chunk.position.axis.function.cd11', 'CDELT1')
        bp.set('Chunk.position.axis.function.cd12', 0.0)
        bp.set('Chunk.position.axis.function.cd21', 0.0)
        bp.add_attribute('Chunk.position.axis.function.cd22', 'CDELT2')

        bp.set('Chunk.energy.bandpassName', 'UHF-band')
        bp.add_attribute('Chunk.energy.restfrq', 'RESTFREQ')
        bp.set("Chunk.energy.specsys", 'TOPOCENT')

        # VP 04-07-22
        # new keywords
        # RACS_BND= 'LOW     '           / OBSERVING BAND (LOW,MID,HIGH)
        # RACS_EPC=                    1 / OBSERVING EPOCH
        # RACS_DR =                    1 / DATA RELEASE
        # DATE-OBS= '2019-04-27T23:50:25.622'
        # DATE-END= '2020-03-28T03:32:16.918'

        # time
        bp.configure_time_axis(5)
        bp.set('Chunk.time.axis.axis.ctype', 'TIME')
        bp.set('Chunk.time.axis.axis.cunit', 'd')
        bp.set('Chunk.time.axis.function.naxis', '1')
        bp.set('Chunk.time.axis.function.delta', 'get_time_axis_delta()')
        bp.set('Chunk.time.axis.function.refCoord.pix', '0.5')
        bp.set(
            'Chunk.time.axis.function.refCoord.val',
            'get_time_axis_val(params)',
        )

        bp.clear('Observation.instrument.keywords')
        bp.add_attribute('Observation.instrument.keywords', 'RACS_BND')
        self._logger.debug('Done accumulate_bp.')

    def _get_time_val(self, keyword, ext):
        dateobs = self._headers[ext].get(keyword)
        if dateobs is not None:
            result = ac.get_datetime_mjd(dateobs)
            if result is not None:
                return result
            else:
                return None

    def get_position_resolution(self, ext):
        bmaj = self._headers[ext]['BMAJ']
        bmin = self._headers[ext]['BMIN']
        # From
        # https://open-confluence.nrao.edu/pages/viewpage.action?pageId=13697486
        # Clare Chandler via JJK - 21-08-18
        return 3600.0 * sqrt(bmaj * bmin)

    def get_product_type(self, ext):
        if '.rms.' in self._storage_name.file_uri:
            return ProductType.NOISE
        else:
            return ProductType.SCIENCE

    def get_proposal_id(self, ext):
        caom_name = CaomName(self._storage_name.file_uri)
        bits = caom_name.file_name.split('.')
        return f'{bits[0]}.{bits[1]}'

    def get_time_axis_delta(self, ext):
        exptime = self.get_time_exposure(ext)
        return convert_to_days(exptime)

    def get_time_axis_val(self, ext):
        return self._get_time_val('DATE-OBS', ext).mjd

    def get_time_exposure(self, ext):
        date_obs = self._get_time_val('DATE-OBS', ext)
        date_end = self._get_time_val('DATE-END', ext)
        result = None
        if date_obs is not None and date_end is not None:
            result = date_end.value - date_obs.value
        return result

    def _update_artifact(self, artifact):
        if artifact.uri.startswith('vos:cirada'):
            old_uri = artifact.uri
            artifact.uri = old_uri.replace('vos:cirada', 'vos://cadc.nrc.ca~vault/cirada')
            self._logger.info(f'Change URI from {old_uri} to {artifact.uri}')
