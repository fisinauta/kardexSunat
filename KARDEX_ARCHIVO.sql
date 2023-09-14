DECLARE
   type tabla_tipo_inv is table of EINTERFACE.KD_KARDET_ARCHIVO_PVO%rowtype;
   datos_VENTAS tabla_tipo_inv;

 fecha varchar2(10);
 prd_lvl_child_temp number;
 tda_sunat_temp varchar2(10);

 can_diferencia number;
 costo_diferencia number;
 saldo_uni_inicial number;
 saldo_cos_inicial number;


cursor nuevos_datos(FECHAINICIO VARCHAR2, FECHAFIN VARCHAR2,pcod_emp varchar2) is
SELECT  Q.* FROM
( SELECT  
 --<0004>
   PERIODO,
 --</0004>
       CUO ,
       TIPO_ASIENTO,
      to_char(f8.valor2) COD_ESTABLECIMIENTO,
       to_char(f1.valor) COD_CATALOGO,
       --f2.valor TIPO_EXISTENCIA,
       TIPO_EXISTENCIA ,
       COD_EXISTENCIA_CATALOGO,

       f3.valor COD_EXISTENCIA_UNICOB,
       FECHA_EMISION,
       TIPO_DOCUMENTO,
       DOCUMENTO_SERIE,
       DOCUMENTO_CORRELATIVO,
       TIPOOPERACION,
       DESCRIPCION_EXISTENCIA,
       f9.valor2 COD_UNI_MEDIDA,
       f4.valor CODIGO_DEVALUACION,
       NVL(TRANS_QTY_IN,0),
       NVL(IN_UNIT,0),
       NVL(TRANS_COS_IN,0)TRANS_COS_IN,
       NVL(TRANS_QTY_OU,0),
       NVL(OUT_UNIT,0),
       NVL(TRANS_COS_OU,0)TRANS_COS_OU,
       NVL(INV_FIN_UNI,0),
       NVL(SALDO_UNIT_FINAL,0),

       --<0004>
       NVL(INV_FIN_CST,0),
       --</0004>
       f5.valor ESTADO_FINAL,
       f6.valor CAMPO_LIBRE,
       0,
       '20100070970'  RUC ,
        T  ,
        ORG_LVL_CHILD  ,
        INV_MRPT_CODE,
        INV_DRPT_CODE,
        trans_ref ,
        trans_ref2,
        1

        --ORDENMOVIMIENTOS



  FROM (



          select DISTINCT
                   to_char(trans_date, 'YYYYMM') Periodo,
                   --'1000'||nvl(CUO.cuo, k.cuo_sunat) CUO,
                   DECODE(CUO.cuo , NULL, NULL , '1000'||'2018'||nvl(CUO.cuo,k.cuo_sunat)) CUO,
                   'M001' TIPO_ASIENTO,
                   k.org_lvl_child COD_ESTABLECIMIENTO,
                   TP.PRD_LVL_NUMBER COD_EXISTENCIA_CATALOGO,
                   DECODE(TP.COD_TIPNEG, '094','02','095','03','092','05','01') TIPO_EXISTENCIA ,
                   TO_CHAR(TRANS_DATE, 'DD/MM/YYYY') FECHA_EMISION,

                   case when K.TIP_DOC='0' then '12'
                     else
                       K.TIP_DOC
                       end


                    TIPO_DOCUMENTO,
                   lpad(nvl(substr(K.DOC_NUM,1,4),'0000'),4,'0') DOCUMENTO_SERIE,

                   case when K.TIP_DOC='0' or K.TIP_DOC='12' then
                     substr(to_char(rownum),1,8)
                   else
                    to_char(to_number(substr(nvl(K.DOC_NUM,'1'),6,20)))
                   end
                    DOCUMENTO_CORRELATIVO,
                   SUBSTR(K.CUO_SUNAT, 0, 2) TIPOOPERACION,
                   K.TRANS_QTY_IN,
                   K.TRANS_COS_IN / (CASE
                     WHEN K.TRANS_QTY_IN = 0 THEN
                      1
                      ELSE
                      K.TRANS_QTY_IN
                   END) IN_UNIT,
                   K.TRANS_COS_IN,
                   K.TRANS_QTY_OU,
                   K.TRANS_COS_OU / (CASE
                     WHEN K.TRANS_QTY_OU = 0 THEN
                      1
                     ELSE
                      K.TRANS_QTY_OU
                   END) OUT_UNIT,
                   K.TRANS_COS_OU,
                    0 INV_FIN_UNI,
                   /*SALDO.INV_FIN_CST / (CASE
                     WHEN SALDO.INV_FIN_UNI = 0 THEN
                      1
                     ELSE
                      SALDO.INV_FIN_UNI
                   END)*/ 0 SALDO_UNIT_FINAL,
                   0 INV_FIN_CST,
                   K.TRN_TECH_KEY ORDENMOVIMIENTOS,
                   tp.prd_full_name DESCRIPCION_EXISTENCIA,
--<0004>
                   '20100070970' RUC ,
                   K.ORG_LVL_CHILD ORG_LVL_CHILD,
                   1 T,

                   K.INV_MRPT_CODE,
                   K.INV_DRPT_CODE,
                   '' trans_ref ,
                   '' trans_ref2,
                   TP.COD_UMV
--</0004>
              from EINTERFACE.kd_kardet_vt_PVO k
              inner join EDSR.IFHPRDMST tp
                      on tp.prd_lvl_child=k.trans_prd_child
        LEFT OUTER  JOIN EINTERFACE.KD_KARDET_CUO_PVO CUO
                      ON CUO.cod_operacion = K.RSL_LIN_DEF_KEY and cuo.periodo=to_char(trans_date, 'YYYYMM') and k.cod_emp='01'
                     AND CUO.tipo='M2'
                   where k.trans_date BETWEEN to_date(FechaInicio, 'YYYYMMDD') AND to_date(FechaFin, 'YYYYMMDD')
                     and k.cod_emp='01'
                    -- select *  from kd_kardet_archivo where periodo = '201711'
                   --select *  from orgmstee 
                     --and K.ORG_LVL_CHILD    in ( 2269, 2225, 2219 )
--select *  from orgmstee
              union all

             select DISTINCT 
                   tO_CHAR(TRANS_DATE, 'YYYYMM') "Periodo",
                   --'1000'||'2016'||nvl(CUO.cuo,k.cuo_sunat) CUO,
                   DECODE(CUO.cuo , NULL, NULL , '1000'||'2018'||nvl(CUO.cuo,k.cuo_sunat)) CUO,
                   'M001' TIPO_ASIENTO,
                   k.org_lvl_child COD_ESTABLECIMIENTO,
                   TP.PRD_LVL_NUMBER COD_EXISTENCIA_CATALOGO,
                   DECODE(TP.COD_TIPNEG, '094','02','095','03','092','05','01') TIPO_EXISTENCIA ,
                   TO_CHAR(TRANS_DATE, 'DD/MM/YYYY') FECHA_EMISION,
                   nvl(K.TIP_DOC,'00')  TIPO_DOCUMENTO,
                  case when K.CUO_SUNAT='01' then
                    lpad(( substr(to_char(replace(nvl(K.DOC_NUM,'0000'),'-','')),1,4) ),4,'0')
                    else
                    case when  K.TIP_DOC='12' then
                       '0001'
                    else

                    '0000'
                    end
                    end
                    DOCUMENTO_SERIE,
                    case when K.CUO_SUNAT='01' then
                      substr(to_char(rownum),1,8)
                    else
                      '0000'
                    end


                   DOCUMENTO_CORRELATIVO,
                   SUBSTR(K.CUO_SUNAT, 0, 2) TIPOOPERACION,
                   K.TRANS_QTY_IN,
                   K.TRANS_COS_IN / (CASE
                     WHEN K.TRANS_QTY_IN = 0 THEN
                      1
                     ELSE
                      K.TRANS_QTY_IN
                   END),
                   K.TRANS_COS_IN,
                   K.TRANS_QTY_OU,
                   K.TRANS_COS_OU / (CASE
                     WHEN K.TRANS_QTY_OU = 0 THEN
                      1
                     ELSE
                      K.TRANS_QTY_OU
                   END),
                   K.TRANS_COS_OU,
                   0 INV_FIN_UNI,
                   /*SALDO.INV_FIN_CST / (CASE
                     WHEN SALDO.INV_FIN_UNI = 0 THEN
                      1
                     ELSE
                      SALDO.INV_FIN_UNI
                   END),*/0 SALDO_UNIT_FINAL,
                   0 INV_FIN_CST,
                   K.AUDIT_NUMBER,
                     tp.prd_full_name DESCRIPCION_EXISTENCIA,
                     '20100070970' RUC,
                      K.ORG_LVL_CHILD ORG_LVL_CHILD,
                      1 T,
                      K.INV_MRPT_CODE,
                      K.INV_DRPT_CODE,
                      K.trans_ref ,
                      K.trans_ref2,
                      TP.COD_UMV
                      
                      from EINTERFACE.kd_kardet_otros_PVO k
                      --SELECT *  FROM kd_kardet_otros
                inner join EDSR.IFHPRDMST tp on tp.prd_lvl_child=k.trans_prd_child
               
                LEFT OUTER JOIN EINTERFACE.KD_KARDET_CUO_PVO CUO
                        ON CUO.cod_operacion = K.RSL_LIN_DEF_KEY and cuo.periodo=to_char(trans_date, 'YYYYMM') and k.cod_emp='01'
                       AND CUO.tipo='M2'
                     where k.trans_date BETWEEN to_date(FechaInicio, 'YYYYMMDD') AND to_date(FechaFin, 'YYYYMMDD')
                       and k.cod_emp='01'
                      -- and K.ORG_LVL_CHILD    in ( 2269, 2225, 2219 )
--select *  from orgmstee where org_lvl_number = 
--select *  from EINTERFACE.hp_kardet_valores_fijos
            ) S

   inner join EINTERFACE.hp_kardet_valores_fijos f1 on f1.id=1
    inner join EINTERFACE.hp_kardet_valores_fijos f2 on f2.id=2
    inner join EINTERFACE.hp_kardet_valores_fijos f3 on f3.id=3
    inner join EINTERFACE.hp_kardet_valores_fijos f4 on f4.id=4
    inner join EINTERFACE.hp_kardet_valores_fijos f5 on f5.id=5
    inner join EINTERFACE.hp_kardet_valores_fijos f6 on f6.id=6
    inner join EINTERFACE.hp_kardet_valores_fijos f7 on f7.id=7
    --inner JOIN EINTERFACE.hp_kardet_valores_fijos F8 ON F8.ID BETWEEN 8 AND 48 AND F8.VALOR=TO_CHAR(S.SUCURSAL)
   inner JOIN EINTERFACE.hp_kardet_valores_fijos F8 ON F8.ID BETWEEN 8 AND 703 AND F8.VALOR = TO_CHAR(GET_ORG_EQUIV_SAP_PMM_2(S.COD_ESTABLECIMIENTO))
 --MMG27062017 SE AGREGÓ CONVRERSIÓN DE UNIDADES DE MEDIDA
  inner JOIN EINTERFACE.hp_kardet_valores_fijos F9 ON F9.ID BETWEEN 3000 AND 3005 AND F9.VALOR = S.COD_UMV
     ORDER BY

              COD_EXISTENCIA_CATALOGO ASC,
              COD_ESTABLECIMIENTO     ASC,
              RPAD(Periodo,8,'0')     ASC,
              T ASC,
              FECHA_EMISION           ASC,

              ORDENMOVIMIENTOS        DESC
) Q 
;


    nsecuencia NUMBER;



   BEGIN
     select SEQ_KD_KARDET_CUO.NEXTVAL  into nsecuencia from dual;
     execute immediate 'alter sequence SEQ_KD_KARDET_CUO increment by -' || to_char(nsecuencia) || '' ;
     select SEQ_KD_KARDET_CUO.NEXTVAL  into nsecuencia from dual;
     execute immediate 'alter sequence SEQ_KD_KARDET_CUO increment by 1' ;


     select SEQ_KD_KARDET_ARCHIVO.NEXTVAL  into nsecuencia from dual;
     execute immediate 'alter sequence SEQ_KD_KARDET_ARCHIVO increment by -' || to_char(nsecuencia) || '' ;
     select SEQ_KD_KARDET_ARCHIVO.NEXTVAL  into nsecuencia from dual;
     execute immediate 'alter sequence SEQ_KD_KARDET_ARCHIVO increment by 1' ;


   --<0004>
 --  EXECUTE IMMEDIATE 'TRUNCATE TABLE kd_kardet_archivo';
   --</0004>
--seelct }  from einterface.KD_KARDET_ARCHIVO_SI 

   open nuevos_datos('20181211','20181231' ,'01' );
loop

  fetch nuevos_datos bulk collect into datos_VENTAS limit 10000;
  for i in 1 .. datos_VENTAS.count
  loop

 if
          (

          tda_sunat_temp=datos_VENTAS(i).COD_ESTABLECIMIENTO
          and prd_lvl_child_temp=datos_VENTAS(i).COD_EXISTENCIA_CATALOGO
          )
       then

          can_diferencia:=datos_VENTAS(i).CANTIDAD_INGRESO + datos_VENTAS(i).CANTIDAD_RETIRO;
          costo_diferencia:=datos_VENTAS(i).COSTO_TOTAL_INGRESO + datos_VENTAS(i).COSTO_TOTAL_RETIRADO;
          saldo_uni_inicial:=saldo_uni_inicial+can_diferencia;
          saldo_cos_inicial:=saldo_cos_inicial+costo_diferencia;
       else
          fecha:=datos_VENTAS(i).FECHA_EMISION;
          tda_sunat_temp:=datos_VENTAS(i).COD_ESTABLECIMIENTO;
          prd_lvl_child_temp:=datos_VENTAS(i).COD_EXISTENCIA_CATALOGO;
          saldo_uni_inicial:=datos_VENTAS(i).CANT_UNIDADES_SALDOFINAL;
          saldo_cos_inicial:=datos_VENTAS(i).COSTO_TOTAL_SALDOFINAL;

          can_diferencia:=datos_VENTAS(i).CANTIDAD_INGRESO + datos_VENTAS(i).CANTIDAD_RETIRO;
          costo_diferencia:=datos_VENTAS(i).COSTO_TOTAL_INGRESO + datos_VENTAS(i).COSTO_TOTAL_RETIRADO;



          saldo_uni_inicial:=saldo_uni_inicial+can_diferencia;
          saldo_cos_inicial:=saldo_cos_inicial+costo_diferencia;



          end if;


    insert into EINTERFACE.KD_KARDET_ARCHIVO_2018
  (
    PERIODO ,
    CUO ,
    TIPO_ASIENTO  ,
    COD_ESTABLECIMIENTO ,
    COD_CATALOGO  ,
    TIPO_EXISTENCIA,
    COD_EXISTENCIA_CATALOGO ,
    COD_EXISTENCIA_UNICOB ,
    FECHA_EMISION ,
    TIPO_DOCUMENTO ,
    DOCUMENTO_SERIE ,
    DOCUMENTO_CORRELATIVO ,
    TIPO_OPERACION  ,
    DESCRIPCION_EXISTENCIA  ,
    COD_UNI_MEDIDA  ,
    CODIGO_DEVALUACION  ,
    CANTIDAD_INGRESO  ,
    COSTO_UNITARIO_INGRESO  ,
    COSTO_TOTAL_INGRESO ,
    CANTIDAD_RETIRO ,
    COSTO_UNITARIO_RETIRO ,
    COSTO_TOTAL_RETIRADO  ,
    CANT_UNIDADES_SALDOFINAL  ,
    COSTO_UNIT_SALDOFINAL ,
    COSTO_TOTAL_SALDOFINAL  ,
    ESTADO_FINAL  ,
    CAMPO_LIBRE,
    SECUENCIA_NUMERICA,
    RUCEMPRESA,
    ORG_LVL_CHILD,
    INV_MRPT_CODE,
    INV_DRPT_CODE,
    trans_ref ,
    trans_ref2,
    IND_SISTEMA
  )
   values
   (

    datos_VENTAS(i).PERIODO ,
    datos_VENTAS(i).CUO   ,
    datos_VENTAS(i).TIPO_ASIENTO  ,
    datos_VENTAS(i).COD_ESTABLECIMIENTO ,
    datos_VENTAS(i).COD_CATALOGO  ,
    datos_VENTAS(i).TIPO_EXISTENCIA,
    datos_VENTAS(i).COD_EXISTENCIA_CATALOGO ,
    datos_VENTAS(i).COD_EXISTENCIA_UNICOB ,
    datos_VENTAS(i).FECHA_EMISION ,
    datos_VENTAS(i).TIPO_DOCUMENTO ,
    datos_VENTAS(i).DOCUMENTO_SERIE ,
    datos_VENTAS(i).DOCUMENTO_CORRELATIVO ,
    datos_VENTAS(i).TIPO_OPERACION  ,
    datos_VENTAS(i).DESCRIPCION_EXISTENCIA  ,
    datos_VENTAS(i).COD_UNI_MEDIDA  ,
    datos_VENTAS(i).CODIGO_DEVALUACION  ,
    datos_VENTAS(i).CANTIDAD_INGRESO  ,
    datos_VENTAS(i).COSTO_UNITARIO_INGRESO  ,
    datos_VENTAS(i).COSTO_TOTAL_INGRESO ,
    datos_VENTAS(i).CANTIDAD_RETIRO ,
    datos_VENTAS(i).COSTO_UNITARIO_RETIRO ,
    datos_VENTAS(i).COSTO_TOTAL_RETIRADO  ,
    saldo_uni_inicial  ,    
    DECODE(saldo_uni_inicial,0,0,
    saldo_cos_inicial/saldo_uni_inicial
    ) ,
    saldo_cos_inicial ,
    datos_VENTAS(i).ESTADO_FINAL  ,
    datos_VENTAS(i).CAMPO_LIBRE,
    SEQ_KD_KARDET_ARCHIVO.NEXTVAL,
     datos_VENTAS(i).RUCEMPRESA,
     datos_VENTAS(i).ORG_LVL_CHILD,
     datos_VENTAS(i).INV_MRPT_CODE     ,
    datos_VENTAS(i).INV_DRPT_CODE     ,
    datos_VENTAS(i).trans_ref     ,
    datos_VENTAS(i).trans_ref2    ,
    datos_VENTAS(i).IND_SISTEMA
   );

   END LOOP;
   COMMIT;
  EXIT   WHEN nuevos_datos%NOTFOUND;
end loop;
close nuevos_datos;
END;
