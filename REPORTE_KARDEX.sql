--cambios de prueba
SET SERVEROUTPUT ON;
DECLARE
ORGANIZACION VARCHAR2(100);
SENTENCIA VARCHAR2(10000);
BEGIN
ORGANIZACION:='&ORGANIZACION';
IF LENGTH(ORGANIZACION)>0 THEN
ORGANIZACION:='_'||ORGANIZACION;
END IF;
SENTENCIA:=
'
DECLARE
ARCHIVO  utl_file.file_type;
PERIODO NUMBER;
CURSOR REPORTE(PERIODO NUMBER) IS
SELECT ORG_LVL_NUMBER,ORG_NAME_FULL,COD_TIP_OPR,DES_TIP_OPR, REPLACE(SUM(MONTO),'','',''.'') MONTO FROM
(
    SELECT O.ORG_LVL_NUMBER,O.ORG_NAME_FULL,V.COD_TIP_OPR,M.DES_TIP_OPR,SUM(V.TRANS_COS_IN+V.TRANS_COS_OU) MONTO FROM EINTERFACE.KD_KARDET_VT'||ORGANIZACION||' V
    INNER JOIN EPMM.ORGMSTEE O ON O.ORG_LVL_CHILD = V.ORG_LVL_CHILD
    INNER JOIN EINTERFACE.KD_TIPOPRMAE_PVO M ON M.COD_TIP_OPR=V.COD_TIP_OPR
    WHERE EXTRACT(MONTH FROM TRANS_DATE) = SUBSTR(PERIODO,5,2) AND EXTRACT(YEAR FROM TRANS_DATE) = SUBSTR(PERIODO,1,4)
    GROUP BY O.ORG_LVL_NUMBER,O.ORG_NAME_FULL,V.COD_TIP_OPR,M.DES_TIP_OPR 
    UNION ALL
    SELECT  O.ORG_LVL_NUMBER,O.ORG_NAME_FULL,V.COD_TIP_OPR,M.DES_TIP_OPR,SUM(V.TRANS_COS_IN+V.TRANS_COS_OU) MONTO FROM EINTERFACE.KD_KARDET_OTROS'||ORGANIZACION||' V
    INNER JOIN EPMM.ORGMSTEE O ON O.ORG_LVL_CHILD = V.ORG_LVL_CHILD
    INNER JOIN EINTERFACE.KD_TIPOPRMAE_PVO M ON M.COD_TIP_OPR=V.COD_TIP_OPR
    WHERE EXTRACT(MONTH FROM TRANS_DATE) = SUBSTR(PERIODO,5,2) AND EXTRACT(YEAR FROM TRANS_DATE) = SUBSTR(PERIODO,1,4)
    GROUP BY O.ORG_LVL_NUMBER,O.ORG_NAME_FULL,V.COD_TIP_OPR,M.DES_TIP_OPR
)
GROUP BY ORG_LVL_NUMBER,ORG_NAME_FULL,DES_TIP_OPR,COD_TIP_OPR
ORDER BY ORG_LVL_NUMBER ASC,COD_TIP_OPR ASC;

BEGIN 
PERIODO:=''&PERIODO'';
ARCHIVO := utl_file.fopen(''KARDEXSUNAT'',''REPORTE_KARDEX''||'''||ORGANIZACION||'''||''_''||PERIODO||''.csv'' , ''W'');
utl_file.put_line(ARCHIVO,''ORG_LVL_NUMBER''||'',''||''ORG_NAME_FULL''||'',''||''COD_TIP_OPR''||'',''||''DES_TIP_OPR''||'',''||''MONTO'');
FOR REGISTRO IN REPORTE(PERIODO)
    LOOP    
            
            utl_file.put_line(ARCHIVO,REGISTRO.ORG_LVL_NUMBER||'',''||REGISTRO.ORG_NAME_FULL||'',''||REGISTRO.COD_TIP_OPR||'',''||REGISTRO.DES_TIP_OPR||'',''||REGISTRO.MONTO);
            
    END LOOP;
utl_file.fclose(ARCHIVO);
END;
';
EXECUTE IMMEDIATE SENTENCIA;
END;




