version=v_29_fu_gai_neg
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
/Users/dongjian/PycharmProjects/UserDetected/model/liblinear/predict -b 1 /Users/dongjian/data/${features_lines} /Users/dongjian/data/user_feature_model /Users/dongjian/data/user_features_predict


#train_error

cat ~/data/user_features_predict|tail -n+2 | cut -b 1  > ~/data/y_predict
cat ${train_data}_dup|awk 'NR!=1{print $NF}' > y_test
paste y_test y_predict > rs
awk -F \t '{if($1==1 && $2==1 ){print 1}}' rs|wc -l > val # tp
awk -F \t '{if($1==0 && $2==0 ){print 1}}' rs|wc -l >> val # tn
awk -F \t '{if($1==0 && $2==1 ){print 1}}' rs|wc -l >> val # fp
awk -F \t '{if($1==1 && $2==0 ){print 1}}' rs|wc -l >> val # fn