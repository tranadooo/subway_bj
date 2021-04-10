#coding=UTF-8
from Configure import *
from c_Compute_Character_Likeness import *
#计算日期因素相似度
def compute_charas_likeness(data,chara_multi_likeness,data1,data2):
    charas_likeness_dict = {}
    date1 = data1['日期']
    date2 = data2['日期']
    date_likeness = compute_chara_date_likeness(date1, date2)
    charas_likeness_dict["日期"] = date_likeness
    
    #0-1型因素相似度计算
    if len(chara_01) !=0:
        for cha in chara_01:
            cha1 = data1[cha]
            cha2 = data2[cha]
            if cha1==cha2:
                likeness = 1
            else:
                likeness = 0
            charas_likeness_dict[cha] = likeness
    #定性因素相似度计算
    if len(chara_multi) !=0:
        for cha in chara_multi:
            cha1 = data1[cha]
            cha2 = data2[cha]
            chara_multi_likeness_dict = chara_multi_likeness[cha]
            if  cha1==cha2:
                likeness = 1
            elif (cha1,cha2) in chara_multi_likeness_dict:
                likeness = chara_multi_likeness_dict[(cha1,cha2)]
            elif (cha2,cha1) in chara_multi_likeness_dict:
                likeness = chara_multi_likeness_dict[(cha2,cha1)]
            else:
                likeness = -1
            charas_likeness_dict[cha] = likeness
    #定量因素相似度计算
    if len(chara_conti) !=0:
        for cha in chara_conti:
            cha1 = data1[cha]
            cha2 = data2[cha]
            if cha=='日期':
                likeness=date_likeness
            elif  cha1==cha2:
                likeness = 1
            elif cha in chara_conti_segpara_dict:
                [critical_val, big_para, small_para] = chara_conti_segpara_dict[cha]
                likeness = compute_chara_conti_likeness1(data,cha,cha1,cha2,critical_val, big_para, small_para )            
            elif cha == '疫情':
                d = np.abs(cha1-cha2)
                if d>=1:
                    likeness = -0.1875+0.9875/d
                else:
                    likeness = 0.9
            charas_likeness_dict[cha] = likeness
        
    return charas_likeness_dict

def compute_date_likeness_dict(weight_para,date_charas_likeness_dict):
    date_likeness_dict = {}
    for k,v in date_charas_likeness_dict.items():
        chara_likeness_list = []
        for chara in chara_sort_by_importance:
            chara_likeness_list.append(v[chara])
        chara_likeness_array = np.array(chara_likeness_list)
        date_likeness_dict[k] = np.prod(np.power(chara_likeness_array,weight_para))
    return date_likeness_dict
