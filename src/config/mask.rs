pub fn mask_database_url(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .map(|u| {
            let scheme = u.scheme();
            let host = u.host_str().unwrap_or("unknown");
            let port = u.port().map_or(String::new(), |p| format!(":{p}"));
            if u.password().is_some() {
                format!("{scheme}://****:****@{host}{port}")
            } else if !u.username().is_empty() {
                format!("{scheme}://{}@{host}{port}", u.username())
            } else {
                format!("{scheme}://{host}{port}")
            }
        })
        .unwrap_or_else(|| "unknown".to_string())
}

pub fn mask_secret(secret: &str) -> String {
    if secret.len() <= 4 {
        "****".to_string()
    } else {
        let visible = &secret[secret.len() - 4..];
        format!("****{visible}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_database_url_with_password() {
        let masked = mask_database_url("mysql://admin:s3cret@dbhost.example.com:3306/mydb");
        assert_eq!(masked, "mysql://****:****@dbhost.example.com:3306");
    }

    #[test]
    fn mask_database_url_without_password() {
        let masked = mask_database_url("mysql://admin@dbhost.example.com:3306/mydb");
        assert_eq!(masked, "mysql://admin@dbhost.example.com:3306");
    }

    #[test]
    fn mask_database_url_no_credentials() {
        let masked = mask_database_url("mysql://dbhost.example.com:3306/mydb");
        assert_eq!(masked, "mysql://dbhost.example.com:3306");
    }

    #[test]
    fn mask_database_url_invalid() {
        let masked = mask_database_url("not-a-url");
        assert_eq!(masked, "unknown");
    }

    #[test]
    fn mask_database_url_no_port() {
        let masked = mask_database_url("mysql://user:pass@dbhost/mydb");
        assert_eq!(masked, "mysql://****:****@dbhost");
    }

    #[test]
    fn mask_secret_short_value() {
        assert_eq!(mask_secret("ab"), "****");
    }

    #[test]
    fn mask_secret_exact_four_chars() {
        assert_eq!(mask_secret("abcd"), "****");
    }

    #[test]
    fn mask_secret_long_value() {
        assert_eq!(mask_secret("mysecretkey123"), "****y123");
    }

    #[test]
    fn mask_secret_five_chars() {
        assert_eq!(mask_secret("abcde"), "****bcde");
    }
}
