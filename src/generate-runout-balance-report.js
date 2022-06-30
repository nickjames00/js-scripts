const fs = require('fs');
const util = require('util');
const appendFile = util.promisify(fs.appendFile);

const { init, releaseConnection } = require('@twicapp/backend/database');
const Employee = require('@twicapp/backend/database/models/Employee');

async function main() {
  await init({
    MONGODB_URL: '',
  });

  const employees = await Employee.aggregate([
    {
      $match: {
        terminated: false,
        is_twic_eligible: true,
        'settings.country': {
          $nin: ['us', 'pr'],
        },
        'company.id': 'microsoft',
      },
    },
    {
      $project: {
        id: 1,
        email: '$personal_info.email',
        external_id: '$personal_info.external_id',
      },
    },
    {
      $lookup: {
        from: 'employee_wallet_transaction',
        let: {
          id: '$id',
        },
        pipeline: [
          {
            $match: {
              created: {
                $gt: new Date('2022-06-30T00:00:00.000Z'),
              },
              transaction_subtype: 'wallet_reset',
              company_wallet_configuration_id:
                'd625f01e-2ad1-47c5-9024-bf59b3d0a3ff',
              $expr: {
                $eq: ['$employee_id', '$$id'],
              },
            },
          },
        ],
        as: 'balance',
      },
    },
    {
      $addFields: { count: { $size: '$balance' } },
    },
    {
      $match: { count: { $gt: 0 } },
    },
  ]);

  const headers =
    'id,external id,email,wallet config id,runout balance,6/30 balance\n';
  const fileName = `${new Date().toISOString()}-runout-balance.csv`;

  await appendFile(fileName, headers);

  for (const employee of employees) {
    await appendFile(
      fileName,
      `${employee.id},${employee.external_id},${employee.email},d625f01e-2ad1-47c5-9024-bf59b3d0a3ff,${employee.balance[0].amount},${employee.balance[0].amount}\n`
    );
  }

  releaseConnection();
}

main();
