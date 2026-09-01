"""
Reintentos de `_rpc_post` — el 502 del 1-sep-2026 costó 30 h de datos.

El ETL murió con `502 Bad Gateway` en sperant-etl-bridge y nadie se enteró hasta
que los números de TunApp no cuadraron contra Meta. Estos tests fijan qué se
reintenta y qué NO, para que un 500 legítimo no quede escondido detrás de tres
reintentos inútiles.

Correr:  python3 tests/test_rpc_retry.py
"""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# El módulo importa psycopg2 al cargar y aquí no se toca ninguna base: se stubea
# para que el test corra en cualquier máquina sin instalar el driver de Postgres.
sys.modules.setdefault("psycopg2", mock.MagicMock())

import requests  # noqa: E402
from etl import sperant_etl as etl  # noqa: E402


def _resp(status: int) -> mock.Mock:
    r = mock.Mock(spec=requests.Response)
    r.status_code = status
    r.text = ""
    return r


class TestReintentos(unittest.TestCase):
    def setUp(self):
        # Sin esperas reales: los tests no deben tardar 26 segundos.
        self._sleep = mock.patch.object(etl.time, "sleep").start()
        self.addCleanup(mock.patch.stopall)
        etl._OIDC_CACHE = "token-falso"
        etl._OIDC_RESUELTO = True

    def test_502_se_reintenta_y_se_recupera(self):
        """El caso real: el puente devuelve 502 y al siguiente intento responde."""
        with mock.patch.object(etl, "_rpc_post_once",
                               side_effect=[_resp(502), _resp(200)]) as m:
            resp = etl._rpc_post("upsert_sperant_leads", "{}")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(m.call_count, 2)

    def test_500_NO_se_reintenta(self):
        """Un 500 es la RPC fallando de verdad: reintentarlo esconde el error."""
        with mock.patch.object(etl, "_rpc_post_once", return_value=_resp(500)) as m:
            resp = etl._rpc_post("upsert_sperant_leads", "{}")
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(m.call_count, 1, "un 500 no se debe reintentar")

    def test_403_NO_se_reintenta(self):
        """RPC fuera de la lista blanca: determinista, no cambia al repetir."""
        with mock.patch.object(etl, "_rpc_post_once", return_value=_resp(403)) as m:
            etl._rpc_post("rpc_prohibida", "{}")
        self.assertEqual(m.call_count, 1)

    def test_se_agotan_los_reintentos_y_devuelve_el_ultimo(self):
        """Si el puente nunca levanta, el llamador recibe el 502 y falla fuerte."""
        with mock.patch.object(etl, "_rpc_post_once", return_value=_resp(502)) as m:
            resp = etl._rpc_post("upsert_sperant_leads", "{}")
        self.assertEqual(resp.status_code, 502)
        self.assertEqual(m.call_count, etl.RPC_REINTENTOS + 1)

    def test_agotar_reintentos_NO_se_loguea_como_recuperado(self):
        """
        Decir «recuperado» al devolver un 502 es el log engañoso que hizo que el
        corte del 1-sep pasara 30 h inadvertido. Al agotar, se loguea ERROR.
        """
        with mock.patch.object(etl, "_rpc_post_once", return_value=_resp(502)), \
             mock.patch.object(etl.log, "error") as err, \
             mock.patch.object(etl.log, "info") as info:
            etl._rpc_post("upsert_sperant_leads", "{}")
        self.assertTrue(err.called, "agotar los reintentos debe loguear ERROR")
        recuperado = [c for c in info.call_args_list if "recuperado" in str(c)]
        self.assertEqual(recuperado, [], "no puede decir «recuperado» con un 502")

    def test_error_de_red_se_reintenta(self):
        with mock.patch.object(
            etl, "_rpc_post_once",
            side_effect=[requests.ConnectionError("boom"), _resp(204)]
        ) as m:
            resp = etl._rpc_post("upsert_sperant_interacciones", "{}")
        self.assertEqual(resp.status_code, 204)
        self.assertEqual(m.call_count, 2)

    def test_error_de_red_persistente_propaga(self):
        """No se traga la excepción: el workflow tiene que ponerse rojo."""
        with mock.patch.object(etl, "_rpc_post_once",
                               side_effect=requests.ConnectionError("boom")):
            with self.assertRaises(requests.ConnectionError):
                etl._rpc_post("upsert_sperant_leads", "{}")

    def test_401_renueva_el_token_oidc_una_sola_vez(self):
        """
        El token OIDC vive ~10 min y se pide UNA vez por corrida. Un 401 con OIDC
        puesto es casi siempre vencimiento — se renueva. Pero sólo una vez: si el
        token nuevo también da 401, es rechazo de verdad y no hay que insistir.
        """
        with mock.patch.object(etl, "_rpc_post_once",
                               side_effect=[_resp(401), _resp(401)]) as m:
            resp = etl._rpc_post("upsert_sperant_leads", "{}")
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(m.call_count, 2, "el 401 se reintenta UNA vez, no tres")

    def test_la_espera_crece_y_lleva_jitter(self):
        with mock.patch.object(etl, "_rpc_post_once", return_value=_resp(503)):
            etl._rpc_post("upsert_sperant_kpis", "{}")
        esperas = [c.args[0] for c in self._sleep.call_args_list]
        self.assertEqual(len(esperas), etl.RPC_REINTENTOS)
        for previa, siguiente in zip(esperas, esperas[1:]):
            self.assertGreater(siguiente, previa, "el backoff debe crecer")
        # Jitter ±20% sobre 2, 6, 18
        for espera, base in zip(esperas, [2.0, 6.0, 18.0]):
            self.assertGreaterEqual(espera, base * 0.8)
            self.assertLessEqual(espera, base * 1.2)

    def test_exito_al_primer_intento_no_duerme(self):
        with mock.patch.object(etl, "_rpc_post_once", return_value=_resp(200)):
            etl._rpc_post("upsert_sperant_leads", "{}")
        self._sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
