from fastapi.testclient import TestClient
from api import app

client = TestClient(app)


polygon1 = {
    'uuid': '65e43aff-2265-4018-b190-448b0dbcaa97',
    'color': '#f4fde2',
    'wkt': 'POLYGON((-5947831.817748386 3856663.282401694,-839895.5592785887 8964599.540871492,805614.1078794673 3273878.6086165477,-5947831.817748386 3856663.282401694))'
}

polygon2 = {
    'uuid': '775994d5-8f6d-4f3e-bed9-2849e9380669',
    'color': '#dea973',
    'wkt': 'POLYGON((-7647165.019711837 -483997.78605771065,-3387984.5024039783 3941124.8293270776,-7591850.987019526 4992091.450480965,-7647165.019711837 -483997.78605771065))'
}


def test_health():
    response = client.get("/health")

    assert response.status_code == 200

    assert 'status' in response.json()


def test_iss_sun_exposures():
    response = client.get("/iss/sun")

    assert response.status_code == 200

    data = response.json()

    if len(data):
        assert all(all(key in sun_exposure for key in [
                   'start', 'end']) for sun_exposure in data['sun_exposures'])
    else:
        assert data == []


def test_iss_position():
    response = client.get("/iss/position")

    assert response.status_code == 200

    data = response.json()

    assert all(key in data for key in [
               'latitude', 'longitude']) or 'message' in data


def test_post_2d_polygon():

    response = client.post('/2d-polygons', json=polygon1)

    assert response.status_code == 200

    assert response.json() == {'message': polygon1['uuid']}

    # not valid polygon wkt

    response = client.post(
        '/2d-polygons', json={'uuid': '1', 'color': '#fffff', 'wkt': 'POLYGON((-7647165.019711837 -483997.'})

    assert response.status_code == 200

    assert response.json() == {
        'message': "The wkt string is not a valid 2D polygon"}

    # not valid param

    response = client.post(
        '/2d-polygons', json={'color': 'red'})

    assert response.status_code == 422


def test_delete_2d_polygon():

    uuid = polygon1['uuid']

    response = client.delete(f'/2d-polygons/{uuid}')

    assert response.status_code == 200

    assert response.json() == {'message': uuid}

    response = client.delete(f'/2d-polygons/{uuid}')

    assert response.status_code == 200

    assert response.json() == {'message': 'No affected rows'}

    # not valid uuid

    response = client.delete('/2d-polygons/1111')

    assert response.status_code == 200

    assert response.json() == {"message": "No affected rows"}


def test_get_2d_polygon():

    uuid = polygon1['uuid']

    response = client.get(f'/2d-polygons/{uuid}')

    assert response.status_code == 200

    assert response.json() == {
        "message": "No polygon with the given uuid exists"}

    client.post('/2d-polygons', json=polygon1)

    response = client.get(f'/2d-polygons/{uuid}')

    assert response.status_code == 200

    assert response.json() == polygon1

    # not valid uuid

    response = client.get('/2d-polygons/1111')

    assert response.status_code == 200

    assert response.json() == {
        'message': 'No polygon with the given uuid exists'}


def test_get_all_2d_polygons():

    response = client.get('/2d-polygons')

    assert response.status_code == 200

    assert response.json()['polygons'] == [polygon1]

    client.post('/2d-polygons', json=polygon2)

    response = client.get('/2d-polygons')

    assert response.status_code == 200

    assert response.json()['polygons'] == [polygon1, polygon2]

    uuid = polygon1['uuid']

    client.delete(f'/2d-polygons/{uuid}')

    response = client.get(f'/2d-polygons')

    assert response.status_code == 200

    assert response.json()['polygons'] == [polygon2]

    uuid = polygon2['uuid']

    client.delete(f'/2d-polygons/{uuid}')

    response = client.get(f'/2d-polygons')

    assert response.status_code == 200

    assert response.json()['polygons'] == []
