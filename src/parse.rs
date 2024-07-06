use crate::command::{StorageCommand, DeleteCommand, GetCommand, PutCommand};

pub fn parse_command(input: String) -> Result<StorageCommand, String> {
    let tokens = Lexer::new(input).lex()?;
    parse_tokens(tokens.into_iter())
}

#[derive(Debug)]
enum Keyword {
    Get,
    Put,
    Delete,
    Exit,
}

#[derive(Debug)]
enum Token {
    Keyword(Keyword),
    Ident(String),
    Literal(String),
}

#[derive(Debug)]
struct Lexer {
    input: Vec<char>,
    pos: usize,
    buffer: String,
    tokens: Vec<Token>,
}

impl Lexer {
    fn new(input: String) -> Lexer {
        Lexer {
            input: input.chars().collect(),
            pos: 0,
            buffer: String::new(),
            tokens: Vec::new(),
        }
    }

    fn lex(mut self) -> Result<Vec<Token>, String> {
        while let Some(c) = self.input.get(self.pos) {
            if c.is_whitespace() {
                self.pos += 1;
            } else if c.is_alphanumeric() {
                self.lex_alphanumeric()?;
            } else if c == &'"' {
                self.lex_literal()?;
            } else {
                return Err(format!("Unexpected character: {}", c));
            }
        }
        Ok(self.tokens)
    }

    fn lex_alphanumeric(&mut self) -> Result<(), String> {
        while let Some(c) = self.input.get(self.pos) {
            self.pos += 1;
            if c.is_alphanumeric() {
                self.buffer.push(*c);
            } else if c.is_whitespace() {
                break;
            } else {
                return Err(format!("Unexpected character: {}", c));
            }
        }

        match self.buffer.as_str() {
            "GET" | "get" => {
                self.tokens.push(Token::Keyword(Keyword::Get));
            }
            "PUT" | "put" => {
                self.tokens.push(Token::Keyword(Keyword::Put));
            }
            "DELETE" | "delete" => {
                self.tokens.push(Token::Keyword(Keyword::Delete));
            }
            "EXIT" | "exit" => {
                self.tokens.push(Token::Keyword(Keyword::Exit));
            }
            _ => {
                self.tokens.push(Token::Ident(self.buffer.clone()));
            }
        }
        self.buffer.clear();
        Ok(())
    }

    fn lex_literal(&mut self) -> Result<(), String> {
        // Skip the first '"'
        self.pos += 1;

        // Flag to indicate if the next character is escaped
        let mut is_escaped = false;

        while let Some(c) = self.input.get(self.pos) {
            self.pos += 1;
            if is_escaped {
                match c {
                    '"' | 'n' | 't' | '\\' => {
                        self.buffer.push(*c);
                        is_escaped = false;
                        continue;
                    }
                    _ => {
                        return Err(format!("Invalid escaped character: {}", c));
                    }
                }
            } else {
                match c {
                    '"' => {
                        self.tokens.push(Token::Literal(self.buffer.clone()));
                        self.buffer.clear();
                        return Ok(());
                    }
                    '\\' => {
                        is_escaped = true;
                        continue;
                    }
                    _ => {
                        self.buffer.push(*c);
                    }
                }
            }
        }

        Err(format!("Unexpected end of input, {}", self.buffer))
    }
}

fn parse_tokens(mut tokens: impl Iterator<Item = Token>) -> Result<StorageCommand, String> {
    let next = tokens.next();
    if next.is_none() {
        return Err("Unexpected end of input".into());
    }
    let next = next.unwrap();
    match next {
        Token::Keyword(keyword) => match keyword {
            Keyword::Get => process_get_keyword(&mut tokens),
            Keyword::Put => process_put_keyword(&mut tokens),
            Keyword::Delete => process_delete_keyword(&mut tokens),
            Keyword::Exit => Ok(StorageCommand::Flush),
        },
        _ => Err("Expected keyword GET, PUT or DELETE".into()),
    }
}

fn parse_identifier(
    tokens: &mut impl Iterator<Item = Token>,
    keyword: &str,
) -> Result<String, String> {
    match tokens.next() {
        Some(Token::Ident(ident)) => Ok(ident),
        _ => Err(format!("Expected identifier after {}", keyword)),
    }
}

fn process_put_keyword_with_key(
    ident: String,
    tokens: &mut impl Iterator<Item = Token>,
) -> Result<StorageCommand, String> {
    match tokens.next() {
        Some(Token::Literal(literal)) => {
            if tokens.next().is_some() {
                return Err("Unexpected token after literal".to_string());
            }
            Ok(StorageCommand::Put(PutCommand(ident, literal)))
        }
        _ => Err("Expected literal after identifier".to_string()),
    }
}

fn process_put_keyword(tokens: &mut impl Iterator<Item = Token>) -> Result<StorageCommand, String> {
    let ident = parse_identifier(tokens, "PUT")?;
    process_put_keyword_with_key(ident, tokens)
}

fn process_get_keyword(tokens: &mut impl Iterator<Item = Token>) -> Result<StorageCommand, String> {
    let ident = parse_identifier(tokens, "GET")?;
    if tokens.next().is_some() {
        return Err("Unexpected token after identifier".to_string());
    }
    Ok(StorageCommand::Get(GetCommand(ident)))
}

fn process_delete_keyword(tokens: &mut impl Iterator<Item = Token>) -> Result<StorageCommand, String> {
    let ident = parse_identifier(tokens, "DELETE")?;
    if tokens.next().is_some() {
        return Err("Unexpected token after identifier".to_string());
    }
    Ok(StorageCommand::Delete(DeleteCommand(ident)))
}
