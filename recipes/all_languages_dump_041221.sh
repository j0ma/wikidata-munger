#!/usr/bin/env bash

set -euo pipefail

# Dumping everything, 2/5/2021

LANGS="nia,trv,uz-latn,kum,hu-formal,ase,tzm,ses,de-formal,shy-latn,uz-cyrl,nod,ks-arab,tly,kbd-cyrl,sty,ug-arab,ruq,ii,ko-kp,niu,krl,kri,crh-cyrl,lki,mni,rwr,ks-deva,kjp,jut,srq,dz,rif,pdt,arn,lus,skr-arab,gan-hant,mo,ike-cans,dtp,kk-cn,kk-arab,hif-latn,rmy,anp,shi-latn,cho,to,ruq-latn,ug-latn,aa,shi,kr,arq,liv,ng,egl,mad,sjd,sju,tt-latn,nys,bgn,tw,bqi,fit,glk,ks,kk-latn,nv,rn,cps,krj,ty,bi,gom,gom-deva,grc,tn,gan-hans,kk-cyrl,chy,nl-informal,ike-latn,ny,rgn,bbc,prg,frc,smj,sje,awa,kk-tr,rmf,rm,ik,pcd,tay,ff,szy,ho,lld,sr-ec,pfl,sn,shi-tfng,sr-el,sg,st,pap,de-at,om,bm,avk,ki,lg,lfn,zgh,stq,zh-yue,zea,simple,vot,lv,als,cv,rup,bar,haw,bn,vi,fj,dty,yi,th,ar,hu,sco,tr,aeb,ksh,gor,se,ts,vo,hr,ku,myv,wuu,ta,hil,kbd,it,sd,nb,xal,ti,lij,pl,bo,lzh,zh-hk,gan,hsb,jam,zh-hans,ady-cyrl,bg,fkv,ml,mh,ku-latn,mg,nqo,sa,ast,hrx,ig,sli,vep,ay,pam,khw,mnw,olo,en-ca,kn,sw,bjn,ce,my,ami,tum,kk-kz,fi,sm,sq,lzz,tt,atj,scn,ext,hyw,cdo,pnb,fy,bbc-latn,he,de,gcr,mzn,aln,yue,ruq-cyrl,tet,sh,tt-cyrl,kaa,zh-sg,tyv,nan,shn,kj,hz,ur,crh-latn,btm,lad,hif,mrh,eml,pt,yo,ro,sah,pi,et,af,ch,am,tg-cyrl,vro,pms,tg-latn,zh-tw,loz,cr,vec,ga,lbe,sk,sgs,ia,arc,lrc,lmo,nrm,kw,gn,be-tarask,so,bto,frr,si,ss,br,dsb,kab,bpy,oc,aeb-arab,nds,sl,kiu,sdh,cy,fa,es,be,luz,fur,pih,gom-latn,tl,nn,mwl,mrj,ckb,lt,zh-hant,an,uz,wa,ban,eu,frp,hak,nds-nl,sei,lo,te,din,la,szl,eo,sat,sc,tpi,ms,ba,crh,nah,min,no,kea,ku-arab,ko,is,chr,ca,ht,tcy,ve,ne,kbp,ps,ja,tru,as,ru,zu,bcl,koi,nl,gag,ota,qug,mai,map-bms,nso,co,rue,iu,hi,mt,li,es-formal,cbk-zam,ab,vls,pnt,new,ak,ka,lb,sms,brh,jbo,inh,arz,su,war,pag,id,bcc,uk,aeb-latn,zh-mo,ln,gl,ug,got,pt-br,roa-tara,zh-my,sdc,os,skr,gu,en-gb,diq,sv,wo,gsw,tk,bs,bug,mi,udm,mdf,tg,zh,pa,nap,es-419,csb,lez,ceb,dag,xh,abs,rw,hy,de-ch,ilo,gv,ky,dv,kk,ady,mn,da,mhr,kg,km,bho,qu,kv,cu,el,smn,azb,en,krc,ltg,ha,fo,xsy,ace,nov,zh-cn,xmf,fr,az,cs,vmf,na,srn,mr,ary,gd,io,sr,za,kl,mus,ie,pdc,bxr,ee,av,or,mk,sma,ang,jv"
FORMAT="${1:-tsv}"
OUTPUT_FOLDER="${2:-../datasets/wikidata/all_languages_dump}"
COMBO_OUTPUT="${OUTPUT_FOLDER}/wikidata_all_langs.${FORMAT}"
DEDUP_OUTPUT="${OUTPUT_FOLDER}/wikidata_all_langs.dedup.${FORMAT}"
#MATRIX_OUTPUT="${OUTPUT_FOLDER}/final_resource.matrix.tsv"

mkdir -p $OUTPUT_FOLDER

dump () {

    CONLL_TYPE=$1
    OUTPUT="${OUTPUT_FOLDER}/${CONLL_TYPE}.${FORMAT}"

    # dump everything into one file
    python scripts/io/wikidata_dump_transliterations.py \
        --strict \
        -t "${CONLL_TYPE}" \
        -l "${LANGS}" \
        -f "${FORMAT}" \
        -d "tab" \
        -o - | tee "${OUTPUT}"

}

combine_output () {

    if [ ${FORMAT} = "jsonl" ]
    then
        # do sth
        # combine all the jsonl files into one big jsonl
        cat $OUTPUT_FOLDER/{PER,LOC,ORG}.jsonl > $COMBO_OUTPUT
    else
        # combine all the tsvs into one big tsv, remembering to 
        # omit the header for everything but the first file
        cat $OUTPUT_FOLDER/PER.tsv > $COMBO_OUTPUT
        tail +2 $OUTPUT_FOLDER/LOC.tsv >> $COMBO_OUTPUT
        tail +2 $OUTPUT_FOLDER/ORG.tsv >> $COMBO_OUTPUT
    fi
}

# first extract everything for each type
for conll_type in "PER" "LOC" "ORG"
do
    dump $conll_type
done

combine_output

# deduplicate the rows by using "trumping rules" to break ties etc.
python scripts/io/deduplicate.py \
    -i $COMBO_OUTPUT \
    -o $DEDUP_OUTPUT \
    -f $FORMAT

# finally create the matrix form
python scripts/io/create_matrix.py \
    -i $DEDUP_OUTPUT \
    -o $MATRIX_OUTPUT
