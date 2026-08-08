use cyclone_attributes::*;

#[network]
#[codec(edge)]
pub struct Player {
    #[network(u32)]
    #[codec(edge)]
    pub hp: u32,

    #[network(string)]
    #[codec(edge)]
    pub name: String,
}
