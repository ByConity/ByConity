// #pragma once

// #include <Transaction/Actions/Action.h>
// #include <Access/MaskingPolicyDataModel.h>

// namespace DB
// {
// class CreateMaskingPolicyAction : public Action
// {
// public:
//     CreateMaskingPolicyAction(const Context & context_, const TxnTimestamp & txn_id_, MaskingPolicyModel & policy_)
//         : Action(context_, txn_id_), policy(policy_)
//     {
//     }

//     ~CreateMaskingPolicyAction() = default;

//     void executeV1(TxnTimestamp commit_time) override;
//     void abort() override;
// private:
//     MaskingPolicyModel & policy;
// };

// }
