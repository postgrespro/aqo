from sensate.pipeline.preprocessing.preprocessing_pipeline import PreprocessingPipeline

testcases = [
    'SELECT p.ra, p.dec, p.u, p.g, p.r, p.i, p.z, p.objid, s.z, s.class FROM #upload u JOIN #x x ON x.up_id = u.up_id JOIN PhotoObjAll AS p ON p.objID = x.objID JOIN SpecObjAll AS s ON p.objID = s.bestObjID ORDER BY x.up_id',
    'SELECT p.ra, p.dec, p.u, p.g, p.r, p.i, p.objid, s.z, s.class FROM #upload u JOIN #x x ON x.up_id = u.up_id JOIN PhotoObjAll AS p ON p.objID = x.objID JOIN SpecObjAll AS s ON p.objID = s.bestObjID ORDER BY x.up_id',
    'SELECT u.up_name as [name], x.objID, p.modelMag_u, p.modelMagErr_u, p.modelMag_g, p.modelMagErr_g, p.modelMag_r, p.modelMagErr_r, p.modelMag_i, p.modelMagErr_i, p.modelMag_z, p.modelMagErr_z, p.psfMag_u, p.psfMagErr_u, p.psfMag_g, p.psfMagErr_g, p.psfMag_r, p.psfMagErr_r, p.psfMag_i, p.psfMagErr_i, p.psfMag_z, p.psfMagErr_z, dbo.fPhotoTypeN(p.type) AS type FROM #upload u JOIN #x x ON x.up_id = u.up_id JOIN PhotoObjAll AS p ON p.objID = x.objID ORDER BY x.up_id',
    'SELECT x.objID, p.ra, p.dec, p.psfMag_u, p.psfMag_g, p.psfMag_r, p.psfMag_i, p.psfMag_z, p.extinction_u, p.extinction_g, p.extinction_r, p.extinction_i, p.extinction_z FROM #upload u JOIN #x x ON x.up_id = u.up_id JOIN PhotoObjAll AS p ON p.objID = x.objID ORDER BY x.up_id',
    'SELECT p.ra, p.dec, p.objid, p.run, p.rerun, p.camcol, p.field, s.z, s.plate, s.mjd, s.fiberID, s.specobjid, s.run2d FROM #upload u JOIN #x x ON x.up_id = u.up_id JOIN PhotoObjAll AS p ON p.objID = x.objID JOIN SpecObjAll AS s ON p.objID = s.bestObjID ORDER BY x.up_id',
    'SELECT p.ra, p.dec, p.objid, p.run, p.rerun, p.camcol, p.field FROM #upload u JOIN #x x ON x.up_id = u.up_id JOIN PhotoObjAll AS p ON p.objID = x.objID ORDER BY x.up_id',
    'SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON o.user_id = u.id WHERE u.age >= 21 AND o.status IN ("paid","shipped") GROUP BY u.name ORDER BY total DESC'
]

if __name__ == "__main__":
    pipeline = PreprocessingPipeline()

    for i, sql_query in enumerate(testcases):
        print(f"🧪 Test case {i+1}:")
        print(f"SQL Query: {sql_query}")
        processed = pipeline(sql_query)
        print(f"Processed Output: {processed}")
        print("-" * 50)