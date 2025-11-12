CREATE OR REPLACE procedure HCUE_AMED.pr_crg_hcue_amed_vac_pras as 
/*========================================================================================================================
Fecha Creación:04-Octubre-2024
Creado por:Carlos Chávez
Descripción:Procedimiento que copia las vacunas_COVID del esquema HCUE_AMED desde el servidor de PRODUCCION hacia el servidor de DB_VACUNACION.
Parámetros: No aplica
actualizado por: Ing. Rolando Casigna
Fecha actualización:   10-Octubre-2024
Descripción actualización: Se agregan las columnas notificador, fecha de registro y dosis aplicada.
*/
begin
INSERT INTO HCUE_AMED.DB_VACUNACION_CONSOLIDADA 
(ANIO_APLICACION, MES_APLICACION, DIA_APLICACION, PUNTO_VACUNACION, UNICODIGO, UNI_NOMBRE, ZONA, DISTRITO, PROVINCIA, CANTON, APELLIDOS, NOMBRES, 
TIPO_IDEN, NUM_IDEN, SEXO, ANIO_NACIMIENTO, MES_NACIMIENTO, DIA_NACIMIENTO, NACIONALIDAD, ETNIA, TEL_CONVEN, TEL_CELULAR, EMAIL, POBLA_VACUNA, 
GRUPO_RIESGO, NOMBRE_VACUNA, LOTE_VACUNA, DOSIS_APLICADA, PROFESIONAL_APLICA, IDEN_PROFESIONAL_APLICA, PACIENTE_AGENDADO, FASE_VACUNA, EDAD_ANIOS, SISTEMA, TOTAL)
(
SELECT
to_char(rg.fechavacunacion,'YYYY'),
to_char(rg.fechavacunacion,'MM'),
to_char(rg.fechavacunacion,'DD'),
rg.PUNTOVACUNACIONDESC,
--'' as PUNTO_VACUNACION,
rg.entidad_id,
ent.NOMBREOFICIAL,
ent_zon.descripcion,
ent_dis.codigo,
ent_prv.descripcion,
ent_can.descripcion,
pr.APELLIDOPATERNO ||' '|| pr.APELLIDOMATERNO,
pr.PRIMERNOMBRE ||' '|| pr.SEGUNDoNOMBRE,
det_cat.descripcion,
pr.NUMEROIDENTIFICACION,
det_cat_sex.descripcion,
to_char(pr.FECHANACIMIENTO,'YYYY'),
to_char(pr.FECHANACIMIENTO,'MM'),
to_char(pr.FECHANACIMIENTO,'DD'),
det_pais.NACIONALIDAD,
--ETNIA
det_cat_nacionalidad.descripcion,
pcte.TELEFONO,
pcte.CELULAR,
pcte.CORREO,
'',
'',
-----------------------------------------------------VACUNACION-----------------
vac.nombrevacuna,
rg.LOTE,
esq.dosis,
rg.VACUNADORNOMBRE,
rg.VACUNADORID,
rg.agendado,
rg.fasevacunacion,
-----------------------------------------------------DATOS_PACIENTE-------------
trunc(months_between(TRUNC(rg.fechavacunacion) ,TRUNC(pr.FECHANACIMIENTO) )/12),
'PRAS',
count(*)
from hcue_amed.registrovacunacion@PRAS_PROD_RH rg
inner join hcue_amed.ESQUEMAVACUNACION@PRAS_PROD_RH esq on esq.id=rg.esquemavacunacion_id
LEFT join hcue_amed.paciente@PRAS_PROD_RH pcte on pcte.id=rg.paciente_id
LEFT join HCUE_SISTEMA.persona@PRAS_PROD_RH pr on pr.id=pcte.persona_id
inner join hcue_amed.vacuna@PRAS_PROD_RH vac on vac.id=esq.vacuna_id
LEFT join hcue_catalogos.detallecatalogo@PRAS_PROD_RH det_cat on  det_cat.id=pr.CTTIPOIDENTIFICACION_ID
--CATALOGO NACIONALIDAD
LEFT join hcue_catalogos.detallecatalogo@PRAS_PROD_RH det_cat_nacionalidad on  det_cat_nacionalidad.id=pcte.CTETNIA_ID
LEFT join hcue_catalogos.pais@PRAS_PROD_RH det_pais on det_pais.id=pr.PAIS_ID
--CATALOGO ENTIDAD
inner join hcue_sistema.entidad@PRAS_PROD_RH ent on ent.id=rg.ENTIDAD_ID
inner join hcue_catalogos.parroquia@PRAS_PROD_RH ent_par on ent_par.id=ent.PARROQUIA_ID
inner join hcue_catalogos.canton@PRAS_PROD_RH ent_can on  ent_par.canton_id=ent_can.id
inner join hcue_catalogos.provincia@PRAS_PROD_RH ent_prv on ent_can.provincia_id=ent_prv.id
inner join hcue_catalogos.circuito@PRAS_PROD_RH ent_circ on ent.circuito_id=ent_circ.id
inner join hcue_catalogos.distrito@PRAS_PROD_RH ent_dis on  ent_circ.distrito_id=ent_dis.id
inner join hcue_catalogos.zona@PRAS_PROD_RH ent_zon on  ent_zon.id=ent_dis.zona_id
--CATALOGO SEXO
LEFT join hcue_catalogos.detallecatalogo@PRAS_PROD_RH det_cat_sex on  det_cat_sex.id=pr.CTSEXO_ID
where
rg.activo=1
and rg.fechavacunacion=to_date(SYSDATE-1,'dd/mm/yyyy')
and(esq.vacuna_id>=21 and esq.vacuna_id<=32) --BNT162b2 (vacuna Pfizer )
--AND rownum <= 3
group by
rg.fechavacunacion,rg.PUNTOVACUNACIONDESC,
rg.entidad_id,ent.nombreoficial,ent_zon.descripcion,ent_dis.codigo,ent_prv.descripcion,ent_can.descripcion,pr.APELLIDOPATERNO,
pr.APELLIDOMATERNO,pr.PRIMERNOMBRE,pr.SEGUNDoNOMBRE,det_cat.descripcion,pr.NUMEROIDENTIFICACION,det_cat_sex.descripcion,pr.FECHANACIMIENTO,
det_pais.NACIONALIDAD,det_cat_nacionalidad.descripcion,pcte.TELEFONO,pcte.CELULAR,pcte.CORREO,vac.nombrevacuna,rg.LOTE,esq.dosis,rg.VACUNADORNOMBRE,
rg.VACUNADORID,rg.AGENDADO,rg.fasevacunacion,rg.fechavacunacion);
end pr_crg_hcue_amed_vac_pras;
;
;
;
;
;
;
;
;