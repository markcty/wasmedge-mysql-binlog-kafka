use mysql::prelude::*;
use mysql::*;

const TABLES: &[&str] = &["paymentA", "paymentB", "paymentC"];

#[derive(Debug, PartialEq, Eq)]
struct Payment {
    customer_id: i32,
    amount: i32,
    account_name: Option<String>,
}
fn main() -> std::result::Result<(), Error> {
    let url = if let Ok(url) = std::env::var("DATABASE_URL") {
        let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
        if opts
            .get_db_name()
            .expect("a database name is required")
            .is_empty()
        {
            panic!("database name is empty");
        }
        url
    } else {
        "mysql://root:password@127.0.0.1:3307/mysql".to_string()
    };
    let pool = Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;

    for table in TABLES {
        // Let's create a table for payments.
        conn.query_drop(format!(
            r"CREATE TEMPORARY TABLE {table} (
        customer_id int not null,
        amount int not null,
        account_name text
    )"
        ))?;

        let payments = vec![Payment {
            customer_id: 1,
            amount: 2,
            account_name: None,
        }];

        // Now let's insert payments to the database
        conn.exec_batch(
            format!(
                r"INSERT INTO {table} (customer_id, amount, account_name)
      VALUES (:customer_id, :amount, :account_name)"
            ),
            payments.iter().map(|p| {
                params! {
                    "customer_id" => p.customer_id,
                    "amount" => p.amount,
                    "account_name" => &p.account_name,
                }
            }),
        )?;

        // Let's select payments from database. Type inference should do the trick here.
        let selected_payments = conn.query_map(
            format!("SELECT customer_id, amount, account_name from {table}"),
            |(customer_id, amount, account_name)| Payment {
                customer_id,
                amount,
                account_name,
            },
        )?;

        // Let's make sure, that `payments` equals to `selected_payments`.
        // Mysql gives no guaranties on order of returned rows
        // without `ORDER BY`, so assume we are lucky.
        assert_eq!(payments, selected_payments);
        dbg!(selected_payments);
    }

    println!("Yay!");

    Ok(())
}
