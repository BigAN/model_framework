version=v_27_zhenghe_pingjia_jizhong
app_name=user_features

train_data=user_feature_raw
app=${app_name}_${version}
features_lines=${app_name}_${version}_features_lines
test_file=user_features_test

#python脚本
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python reconstruct_conf.py
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python gen_feature_ids.py
cd /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector && python fea_extra.py -m train
cd ~/data && head ${features_lines}

cd ~/data &&/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/train -v 5 -e 0.1 -s 7 -c 0.5  ${features_lines}
cd ~/data && rm user_feature_model
/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/train  -e 0.1 -s 7 -c 0.5 /Users/dongjian/data/${features_lines} /Users/dongjian/data/user_feature_model

#test
python /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector/fea_extra.py -m test
/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/predict -b 1 /Users/dongjian/data/${features_lines} /Users/dongjian/data/user_feature_model /Users/dongjian/data/user_features_predict

#print model
features_ids_name=${app}_features_ids
cat $features_ids_name >  user_features_features_ids_sort
tail -n+7 user_feature_model > model_xishu
awk -F '&#&' 'FNR==NR{flag[$2]=1;line[$2]=$0;next}{if(flag[FNR]==1){printf("%s\t%s\n",line[FNR],$0)}}' $features_ids_name model_xishu > model_xishu_ids
sort -t$'\t' -k 2gr model_xishu_ids > model_xishu_ids_sort_${version}


#tran id to name
#把feature_lines 转化为带特征name的
python /Users/dongjian/PycharmProjects/UserDetected/feature_work/convert_vector/tran_id_to_value.py #user_features_features_lines_with_info

#paste
cd ~/data &&awk -F ' ' 'BEGIN{print "value"}{print $0}' ${features_lines}_with_info > user_features_value_for_paste #带上前缀value
cd ~/data &&awk -F ' ' 'BEGIN{print "prob"}NR==1{if($2==0){num=3}else{num=2}}{if(NR!=1){print $num}}' user_features_predict> user_features_for_paste #带上前缀 prob
cd ~/data &&paste user_features_for_paste ${test_file} > user_features_rs #将prob 和原始文件组合起来 user_features 为目标文件 ${test_file}带第一行
cd ~/data &&paste user_features_rs user_features_value_for_paste > ${features_lines}_final #将value 和原始文件组合起来
head -1  ${features_lines}_final

cd ~/data &&awk -F '\t' '{if($1>0.95&&NR!=1){print $0}}' ${features_lines}_final | sort -k 1 -r -n -g > user_features_rs_above_9_tmp && head -n 1 user_features_rs | cat - user_features_rs_above_9_tmp > user_features_rs_above_9_ttmp && mv user_features_rs_above_9_ttmp ${features_lines}_rs_all
wc ${features_lines}_rs_all

#csv
cd ~/data && tr -s "," "#" < ${features_lines}_rs_all |awk -F '\t' 'BEGIN{OFS=","}{$1=$1;print $0}'  > ${features_lines}_rs_prob.csv
(cd ~/data&& awk 'BEGIN{srand()}{b[rand()NR]=$0}END{for(x in b)print b[x]}' ${features_lines}_rs_prob.csv > tmp && head -1 ${features_lines}_rs_prob.csv|cat - tmp > tmp_1 && head -n 1 tmp_1 && tail -n+2  tmp_1|tail -n 60|sort -t$'\t' -k 1rn) >  ${features_lines}_rs_prob_60.csv
cd ~/data && echo ${features_lines}_rs_prob_60.csv && open ${features_lines}_rs_prob_60.csv


#(cd ~/data&& awk 'BEGIN{srand()}{b[rand()NR]=$0}END{for(x in b)print b[x]}' user_features_v_1_16__fix_mean_visit_fir_one_poi_features_lines_rs_prob.csv.tmp > tmp && head -1 user_features_v_1_16__fix_mean_visit_fir_one_poi_features_lines_rs_prob.csv|cat - tmp > tmp_1 && head -n 1 tmp_1 && tail -n+2  tmp_1|tail -n 60|sort -t$'\t' -k 1rn) >  user_features_v_1_16__fix_mean_visit_fir_one_poi_features_lines_rs_prob_60.csv
#cd ~/data && echo user_features_v_1_16__fix_mean_visit_fir_one_poi_features_lines_rs_prob_60.csv && open user_features_v_1_16__fix_mean_visit_fir_one_poi_features_lines_rs_prob_60.csv